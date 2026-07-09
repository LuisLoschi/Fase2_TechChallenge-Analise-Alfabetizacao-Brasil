import pandas as pd
import streamlit as st

from athena_connection import GOLD_TABLE, run_query, sql_literal

CACHE_TTL = 3600  # segundos


def _filtro_territorio(regiao: str | None, uf: str | None) -> str:
    """Monta as cláusulas opcionais de filtro territorial (região e/ou UF)."""
    clauses = []
    if regiao:
        clauses.append(f"AND nome_regiao = {sql_literal(regiao)}")
    if uf:
        clauses.append(f"AND sigla_uf = {sql_literal(uf)}")
    return " ".join(clauses)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_colunas_gold() -> set[str]:
    """Objetivo: descobrir as colunas realmente catalogadas na Gold, para o
    dashboard habilitar a seção de métricas de aluno apenas quando a pipeline
    foi executada com o fluxo de streaming (colunas opcionais)."""
    df = run_query(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = {sql_literal(GOLD_TABLE)}"
    )
    return set(df["column_name"])


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_dim_redes() -> pd.DataFrame:
    """Objetivo: listar os anos e redes de ensino disponíveis na Gold,
    alimentando os filtros de ano e rede da sidebar."""
    return run_query(
        f"SELECT DISTINCT ano, rede, rede_nome FROM {GOLD_TABLE} ORDER BY ano DESC, rede"
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_dim_territorio() -> pd.DataFrame:
    """Objetivo: listar regiões e UFs existentes na Gold, alimentando os
    filtros territoriais da sidebar."""
    return run_query(
        f"SELECT DISTINCT nome_regiao, sigla_uf, nome_uf FROM {GOLD_TABLE} "
        "ORDER BY nome_regiao, sigla_uf"
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_brasil_por_ano() -> pd.DataFrame:
    """Objetivo: obter, por ano, a taxa nacional oficial do INEP e a meta
    nacional (constantes por partição) — base dos KPIs e do gráfico
    'Brasil: realizado × meta'."""
    return run_query(
        f"""
        SELECT ano,
               max(taxa_alfabetizacao_brasil_oficial) AS taxa_oficial,
               max(meta_alfabetizacao_brasil)         AS meta_nacional,
               max(percentual_participacao_brasil)    AS participacao
        FROM {GOLD_TABLE}
        GROUP BY ano
        ORDER BY ano
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_kpis_recorte(ano: str, rede: str, regiao: str | None, uf: str | None) -> pd.DataFrame:
    """Objetivo: consolidar os KPIs do recorte selecionado — total de
    municípios, taxa média e contagem de municípios por status da meta
    (Atingida / Abaixo / Sem meta)."""
    return run_query(
        f"""
        SELECT count(DISTINCT id_municipio)                     AS municipios,
               round(avg(taxa_alfabetizacao_municipio), 2)      AS taxa_media,
               count_if(meta_atingida_municipio = 'Atingida')   AS atingiram,
               count_if(meta_atingida_municipio = 'Abaixo')     AS abaixo,
               count_if(meta_atingida_municipio = 'Sem meta')   AS sem_meta
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          {_filtro_territorio(regiao, uf)}
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_ranking_uf(ano: str, rede: str) -> pd.DataFrame:
    """Objetivo: ranquear as UFs pela taxa de alfabetização no ano/rede
    selecionados, com a meta estadual e o status de atingimento para colorir
    o ranking."""
    return run_query(
        f"""
        SELECT sigla_uf,
               max(nome_uf)               AS nome_uf,
               max(taxa_alfabetizacao_uf) AS taxa_uf,
               max(meta_alfabetizacao_uf) AS meta_uf,
               max(meta_atingida_uf)      AS status_meta
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          AND taxa_alfabetizacao_uf IS NOT NULL
        GROUP BY sigla_uf
        ORDER BY taxa_uf DESC
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_distribuicao(ano: str, rede: str, regiao: str | None, uf: str | None) -> pd.DataFrame:
    """Objetivo: montar o histograma da taxa municipal — quantidade de
    municípios por faixa de 5 p.p. — para mostrar a desigualdade dentro do
    recorte."""
    return run_query(
        f"""
        SELECT cast(floor(taxa_alfabetizacao_municipio / 5) * 5 AS integer) AS faixa,
               count(*) AS municipios
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          AND taxa_alfabetizacao_municipio IS NOT NULL
          {_filtro_territorio(regiao, uf)}
        GROUP BY 1
        ORDER BY 1
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_atingimento_regiao(ano: str, rede: str, regiao: str | None, uf: str | None) -> pd.DataFrame:
    """Objetivo: contar municípios por região e status da meta municipal,
    base do gráfico de barras 100% empilhadas de atingimento por região."""
    return run_query(
        f"""
        SELECT nome_regiao,
               meta_atingida_municipio AS status_meta,
               count(*) AS municipios
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          {_filtro_territorio(regiao, uf)}
        GROUP BY 1, 2
        ORDER BY 1
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_extremos_municipios(
    ano: str, rede: str, regiao: str | None, uf: str | None, ordem: str, n: int = 10
) -> pd.DataFrame:
    """Objetivo: destacar os N municípios de maior (`ordem='top'`) ou menor
    (`ordem='bottom'`) taxa de alfabetização no recorte — os últimos são
    candidatos a priorização de política pública."""
    direcao = "DESC" if ordem == "top" else "ASC"
    return run_query(
        f"""
        SELECT nome_municipio,
               sigla_uf,
               taxa_alfabetizacao_municipio AS taxa,
               meta_alfabetizacao_municipio AS meta,
               meta_atingida_municipio      AS status_meta
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          AND taxa_alfabetizacao_municipio IS NOT NULL
          {_filtro_territorio(regiao, uf)}
        ORDER BY taxa {direcao}
        LIMIT {int(n)}
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_kpis_aluno(ano: str, rede: str, regiao: str | None, uf: str | None) -> pd.DataFrame:
    """Objetivo: consolidar os KPIs dos microdados de aluno no recorte —
    alunos avaliáveis, presença média, taxa de alfabetização observada entre
    os presentes e proficiência média ponderada. Requer a Gold gerada com o
    fluxo de streaming (colunas de aluno)."""
    return run_query(
        f"""
        SELECT sum(alunos_total)                                        AS alunos_total,
               sum(alunos_presentes)                                    AS alunos_presentes,
               sum(alunos_alfabetizados)                                AS alunos_alfabetizados,
               round(sum(alunos_presentes) * 100.0
                     / nullif(sum(alunos_total), 0), 2)                 AS presenca_media,
               round(sum(alunos_alfabetizados) * 100.0
                     / nullif(sum(alunos_presentes), 0), 2)             AS taxa_observada,
               round(avg(proficiencia_media_ponderada), 2)              AS proficiencia_media
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          {_filtro_territorio(regiao, uf)}
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_presenca_aprendizagem(ano: str, rede: str, regiao: str | None, uf: str | None) -> pd.DataFrame:
    """Objetivo: por município, cruzar presença na prova e taxa observada de
    alfabetização, com os status de meta e de presença — base do diagnóstico
    'o problema é participação ou aprendizagem'."""
    return run_query(
        f"""
        SELECT nome_municipio,
               sigla_uf,
               proporcao_presenca,
               taxa_alfabetizacao_observada,
               meta_atingida_municipio           AS status_meta,
               meta_atingida_presenca_municipio  AS status_presenca
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          AND proporcao_presenca IS NOT NULL
          {_filtro_territorio(regiao, uf)}
        LIMIT 6000
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_diagnostico_meta_presenca(ano: str, rede: str, regiao: str | None, uf: str | None) -> pd.DataFrame:
    """Objetivo: contar municípios no cruzamento status da meta × status da
    presença, separando onde a intervenção é pedagógica (aprendizagem) de
    onde é de busca ativa (participação)."""
    return run_query(
        f"""
        SELECT meta_atingida_municipio          AS status_meta,
               meta_atingida_presenca_municipio AS status_presenca,
               count(*)                         AS municipios
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          {_filtro_territorio(regiao, uf)}
        GROUP BY 1, 2
        """
    )


@st.cache_data(ttl=CACHE_TTL, show_spinner="Consultando o Athena…")
def q_tabela_detalhe(
    ano: str, rede: str, regiao: str | None, uf: str | None, busca: str
) -> pd.DataFrame:
    """Objetivo: detalhar município a município o indicador, a meta, o status
    e os comparativos com UF e Brasil — base da tabela final do dashboard
    (com busca por nome e limite de 5.000 linhas)."""
    filtro_busca = (
        f"AND lower(nome_municipio) LIKE {sql_literal('%' + busca.lower() + '%')}"
        if busca else ""
    )
    return run_query(
        f"""
        SELECT id_municipio,
               nome_municipio,
               sigla_uf,
               nome_regiao,
               rede_nome,
               taxa_alfabetizacao_municipio,
               meta_alfabetizacao_municipio,
               meta_atingida_municipio,
               media_portugues_municipio,
               percentual_participacao_municipio,
               taxa_alfabetizacao_uf,
               taxa_alfabetizacao_brasil_oficial
        FROM {GOLD_TABLE}
        WHERE ano = {sql_literal(ano)}
          AND rede = {sql_literal(rede)}
          {_filtro_territorio(regiao, uf)}
          {filtro_busca}
        ORDER BY taxa_alfabetizacao_municipio DESC
        LIMIT 5000
        """
    )
