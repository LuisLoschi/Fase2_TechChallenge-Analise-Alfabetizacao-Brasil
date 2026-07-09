import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from athena_connection import GOLD_TABLE, AthenaQueryError
from queries import (
    q_atingimento_regiao,
    q_brasil_por_ano,
    q_colunas_gold,
    q_diagnostico_meta_presenca,
    q_dim_redes,
    q_dim_territorio,
    q_distribuicao,
    q_extremos_municipios,
    q_kpis_aluno,
    q_kpis_recorte,
    q_presenca_aprendizagem,
    q_ranking_uf,
    q_tabela_detalhe,
)

# ---------------------------------------------------------------------------
# Constantes visuais
# ---------------------------------------------------------------------------
AZUL = "#2a78d6" 
AZUL_CLARO = "#9ec5f4"  
VERDE_STATUS = "#0ca30c"
VERMELHO_STATUS = "#d03b3b"
CINZA_NEUTRO = "#898781"
INK_SECUNDARIO = "#52514e"

CORES_STATUS = {
    "Atingida": VERDE_STATUS,
    "Abaixo": VERMELHO_STATUS,
    "Sem meta": CINZA_NEUTRO,
    "Sem dado": "#c3c2b7",
}
ORDEM_STATUS = ["Atingida", "Abaixo", "Sem meta", "Sem dado"]


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    """Formata percentual no padrão pt-BR (62,8%)."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value:.{decimals}f}%".replace(".", ",")


def fmt_delta_pp(value: float, sufixo: str) -> str:
    """Formata variação em pontos percentuais no padrão pt-BR (+3,3 p.p. …)."""
    return f"{value:+.1f}".replace(".", ",") + f" p.p. {sufixo}"


def fmt_num(value: float | None) -> str:
    """Formata inteiro com separador de milhar pt-BR (5.448)."""
    if value is None or pd.isna(value):
        return "—"
    return f"{int(value):,}".replace(",", ".")


def layout_base(fig: go.Figure, height: int = 380) -> go.Figure:
    """Aplica o chrome padrão dos gráficos (grid recessivo, fundo limpo)."""
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=32, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif'),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    fig.update_xaxes(gridcolor="#e1e0d9", zerolinecolor="#c3c2b7")
    fig.update_yaxes(gridcolor="#e1e0d9", zerolinecolor="#c3c2b7")
    return fig


# ---------------------------------------------------------------------------
# Página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Alfabetização no Brasil",
    page_icon="📚",
    layout="wide",
)

st.title("📚 Alfabetização no Brasil")
st.caption(
    "Indicador Criança Alfabetizada (INEP) — camada Gold consultada via Amazon Athena. "
    "Grão: ano × município × rede (2º ano do Ensino Fundamental)."
)

try:
    dim_redes = q_dim_redes()
    dim_territorio = q_dim_territorio()
    brasil = q_brasil_por_ano()
except AthenaQueryError as exc:
    st.error(
        f"Não foi possível consultar o Athena.\n\n**Detalhe:** {exc}\n\n"
        "Confira as credenciais AWS (`aws sts get-caller-identity`) e as variáveis "
        "`ATHENA_REGION`, `ATHENA_DATABASE`, `ATHENA_WORKGROUP` e `ATHENA_S3_OUTPUT`."
    )
    st.stop()

# ----------------------------- Filtros (sidebar) ---------------------------
st.sidebar.header("Filtros")

anos = sorted(dim_redes["ano"].unique(), reverse=True)
ano = st.sidebar.selectbox("Ano da avaliação", anos, index=0)

redes_ano = dim_redes[dim_redes["ano"] == ano].sort_values("rede")
rede = st.sidebar.selectbox(
    "Rede de ensino",
    redes_ano["rede"].tolist(),
    index=redes_ano["rede"].tolist().index("3") if "3" in redes_ano["rede"].tolist() else 0,
    format_func=lambda r: redes_ano.set_index("rede").loc[r, "rede_nome"],
    help="A meta municipal do Compromisso refere-se à rede Municipal (código 3).",
)

regioes = ["Todas"] + sorted(dim_territorio["nome_regiao"].unique())
regiao_sel = st.sidebar.selectbox("Região", regioes, index=0)
regiao = None if regiao_sel == "Todas" else regiao_sel

ufs_dim = dim_territorio if regiao is None else dim_territorio[dim_territorio["nome_regiao"] == regiao]
ufs = ["Todas"] + ufs_dim["sigla_uf"].tolist()
uf_sel = st.sidebar.selectbox(
    "UF", ufs, index=0,
    format_func=lambda s: s if s == "Todas" else f"{s} — {ufs_dim.set_index('sigla_uf').loc[s, 'nome_uf']}",
)
uf = None if uf_sel == "Todas" else uf_sel

st.sidebar.caption(
    "Os filtros dirigem as consultas ao Athena usando a partição `ano`; "
    "os resultados ficam em cache por 1 hora."
)

# ----------------------------- KPIs ----------------------------------------
kpis = q_kpis_recorte(ano, rede, regiao, uf).iloc[0]
linha_brasil = brasil[brasil["ano"] == ano]
brasil_ano = linha_brasil.iloc[0] if not linha_brasil.empty else None
ano_anterior = str(int(ano) - 1)
linha_anterior = brasil[brasil["ano"] == ano_anterior]

col1, col2, col3, col4 = st.columns(4)

with col1:
    delta = None
    if brasil_ano is not None and not linha_anterior.empty:
        delta = brasil_ano["taxa_oficial"] - linha_anterior.iloc[0]["taxa_oficial"]
    st.metric(
        "Taxa nacional oficial (rede pública)",
        fmt_pct(brasil_ano["taxa_oficial"]) if brasil_ano is not None else "—",
        delta=fmt_delta_pp(delta, f"vs {ano_anterior}") if delta is not None else None,
        help="Indicador Criança Alfabetizada oficial do INEP para o Brasil.",
    )

with col2:
    gap = None
    if brasil_ano is not None and pd.notna(brasil_ano["meta_nacional"]):
        gap = brasil_ano["taxa_oficial"] - brasil_ano["meta_nacional"]
    st.metric(
        f"Meta nacional {ano}",
        fmt_pct(brasil_ano["meta_nacional"]) if brasil_ano is not None else "—",
        delta=fmt_delta_pp(gap, "realizado vs meta") if gap is not None else None,
        help="Meta do Compromisso Nacional Criança Alfabetizada para o ano selecionado. "
             "2023 não possui meta definida.",
    )

with col3:
    st.metric(
        "Municípios no recorte",
        fmt_num(kpis["municipios"]),
        help="Municípios com indicador publicado para o ano, rede e território selecionados.",
    )

with col4:
    com_meta = kpis["atingiram"] + kpis["abaixo"]
    pct_atingiu = (kpis["atingiram"] / com_meta * 100) if com_meta > 0 else None
    st.metric(
        "Municípios que atingiram a meta",
        fmt_pct(pct_atingiu) if pct_atingiu is not None else "Sem meta no ano",
        delta=(f"{fmt_num(kpis['atingiram'])} de {fmt_num(com_meta)} com meta definida"
               if com_meta > 0 else None),
        delta_color="off",
        help="Percentual sobre os municípios do recorte que possuem meta anual cadastrada.",
    )

st.divider()

# ----------------------------- Visão nacional ------------------------------
col_nac, col_rank = st.columns([1, 2])

with col_nac:
    st.subheader("Brasil: realizado × meta")
    st.caption("Taxa nacional oficial do INEP (rede pública) contra a meta anual pactuada.")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=brasil["ano"], y=brasil["taxa_oficial"],
        mode="lines+markers+text",
        name="Taxa oficial",
        text=[fmt_pct(v) for v in brasil["taxa_oficial"]],
        textposition="top center",
        line=dict(color=AZUL, width=2),
        marker=dict(size=9),
    ))
    fig.add_trace(go.Scatter(
        x=brasil["ano"], y=brasil["meta_nacional"],
        mode="lines+markers",
        name="Meta nacional",
        line=dict(color=INK_SECUNDARIO, width=2, dash="dash"),
        marker=dict(size=8, symbol="diamond"),
    ))
    fig.update_yaxes(title=None, ticksuffix="%", rangemode="tozero")
    st.plotly_chart(layout_base(fig, height=340), width="stretch")

with col_rank:
    st.subheader(f"Ranking das UFs — {ano}")
    st.caption(
        "Taxa de alfabetização por UF na rede selecionada; a cor indica o atingimento "
        "da meta estadual (referente à rede pública)."
    )
    ranking = q_ranking_uf(ano, rede)
    if ranking.empty:
        st.info("Sem dados de UF para o recorte selecionado.")
    else:
        ranking = ranking.sort_values("taxa_uf")
        fig = go.Figure()
        for status in ORDEM_STATUS:
            parte = ranking[ranking["status_meta"] == status]
            if parte.empty:
                continue
            fig.add_trace(go.Bar(
                y=parte["sigla_uf"], x=parte["taxa_uf"],
                orientation="h",
                name=status,
                marker=dict(color=CORES_STATUS[status]),
                text=[fmt_pct(v) for v in parte["taxa_uf"]],
                textposition="outside",
                customdata=parte[["nome_uf", "meta_uf"]],
                hovertemplate="%{customdata[0]}<br>Taxa: %{x:.1f}%"
                              "<br>Meta UF: %{customdata[1]:.1f}%<extra>%{fullData.name}</extra>",
            ))
        fig.update_layout(barmode="overlay", bargap=0.25)
        fig.update_yaxes(categoryorder="array", categoryarray=ranking["sigla_uf"].tolist())
        fig.update_xaxes(ticksuffix="%", range=[0, ranking["taxa_uf"].max() * 1.15])
        st.plotly_chart(layout_base(fig, height=max(420, 18 * len(ranking))), width="stretch")

st.divider()

# ----------------------------- Municípios ----------------------------------
col_dist, col_reg = st.columns(2)

with col_dist:
    st.subheader("Distribuição dos municípios por taxa")
    st.caption("Quantidade de municípios por faixa de 5 p.p. no recorte selecionado.")
    dist = q_distribuicao(ano, rede, regiao, uf)
    if dist.empty:
        st.info("Sem dados para o recorte selecionado.")
    else:
        fig = go.Figure(go.Bar(
            x=dist["faixa"], y=dist["municipios"],
            marker=dict(color=AZUL),
            hovertemplate="Faixa %{x}–%{customdata}%<br>%{y} municípios<extra></extra>",
            customdata=dist["faixa"] + 5,
        ))
        if brasil_ano is not None and pd.notna(brasil_ano["meta_nacional"]):
            fig.add_vline(
                x=brasil_ano["meta_nacional"], line_dash="dash", line_color=INK_SECUNDARIO,
                annotation_text=f"Meta nacional {fmt_pct(brasil_ano['meta_nacional'])}",
                annotation_position="top",
            )
        fig.update_xaxes(title="Taxa de alfabetização (%)", dtick=10)
        fig.update_yaxes(title="Municípios")
        st.plotly_chart(layout_base(fig, height=360), width="stretch")

with col_reg:
    st.subheader("Atingimento da meta por região")
    st.caption("Participação dos status de meta municipal em cada região (100% = municípios da região).")
    ating = q_atingimento_regiao(ano, rede, regiao, uf)
    if ating.empty:
        st.info("Sem dados para o recorte selecionado.")
    else:
        totais = ating.groupby("nome_regiao")["municipios"].transform("sum")
        ating = ating.assign(pct=ating["municipios"] / totais * 100)
        fig = go.Figure()
        for status in ORDEM_STATUS:
            parte = ating[ating["status_meta"] == status]
            if parte.empty:
                continue
            fig.add_trace(go.Bar(
                y=parte["nome_regiao"], x=parte["pct"],
                orientation="h",
                name=status,
                marker=dict(color=CORES_STATUS[status],
                            line=dict(color="#ffffff", width=2)),
                customdata=parte["municipios"],
                hovertemplate="%{y} — " + status +
                              "<br>%{x:.1f}% (%{customdata} municípios)<extra></extra>",
            ))
        fig.update_layout(barmode="stack")
        fig.update_xaxes(ticksuffix="%", range=[0, 100])
        st.plotly_chart(layout_base(fig, height=360), width="stretch")

st.subheader("Destaques municipais")
col_top, col_bottom = st.columns(2)


def grafico_extremos(df: pd.DataFrame, titulo: str) -> None:
    if df.empty:
        st.info("Sem dados para o recorte selecionado.")
        return
    df = df.assign(rotulo=df["nome_municipio"] + " (" + df["sigla_uf"] + ")")
    df = df.sort_values("taxa")
    fig = go.Figure(go.Bar(
        y=df["rotulo"], x=df["taxa"],
        orientation="h",
        marker=dict(color=AZUL),
        text=[fmt_pct(v) for v in df["taxa"]],
        textposition="outside",
        customdata=df[["meta", "status_meta"]],
        hovertemplate="%{y}<br>Taxa: %{x:.1f}%<br>Meta: %{customdata[0]:.1f}% "
                      "(%{customdata[1]})<extra></extra>",
    ))
    fig.update_xaxes(ticksuffix="%", range=[0, max(df["taxa"].max() * 1.2, 10)])
    st.plotly_chart(layout_base(fig, height=360), width="stretch")
    st.caption(titulo)


with col_top:
    st.markdown("**Maiores taxas de alfabetização**")
    grafico_extremos(
        q_extremos_municipios(ano, rede, regiao, uf, "top"),
        "10 municípios com maior taxa no recorte.",
    )

with col_bottom:
    st.markdown("**Menores taxas de alfabetização**")
    grafico_extremos(
        q_extremos_municipios(ano, rede, regiao, uf, "bottom"),
        "10 municípios com menor taxa no recorte — candidatos a priorização de política pública.",
    )

st.divider()

# ----------------------------- Presença × aprendizagem ---------------------
# Seção condicionada ao schema real: as métricas de aluno só existem se a
# Gold foi gerada após o fluxo de streaming (o ETL as trata como opcionais).
SAEB_CORTE_ALFABETIZACAO = 743.0

if "proporcao_presenca" in q_colunas_gold():
    st.subheader("Presença × aprendizagem (microdados de aluno)")
    st.caption(
        "Separar quem faltou à prova de quem compareceu e não está alfabetizado "
        "muda a intervenção: busca ativa (participação) versus reforço pedagógico "
        "(aprendizagem)."
    )

    aluno = q_kpis_aluno(ano, rede, regiao, uf).iloc[0]
    a1, a2, a3, a4 = st.columns(4)
    with a1:
        st.metric(
            "Alunos avaliáveis",
            fmt_num(aluno["alunos_total"]),
            help="Matriculados avaliáveis no recorte (microdados do streaming).",
        )
    with a2:
        st.metric(
            "Presença na prova",
            fmt_pct(aluno["presenca_media"]),
            delta=f"{fmt_num(aluno['alunos_presentes'])} presentes",
            delta_color="off",
        )
    with a3:
        st.metric(
            "Taxa observada (entre presentes)",
            fmt_pct(aluno["taxa_observada"]),
            help="Alfabetizados / presentes — calculada dos microdados; "
                 "corrobora o indicador oficial.",
        )
    with a4:
        prof = aluno["proficiencia_media"]
        delta_prof = (prof - SAEB_CORTE_ALFABETIZACAO) if pd.notna(prof) else None
        st.metric(
            "Proficiência média (Saeb)",
            f"{prof:.0f}" if pd.notna(prof) else "—",
            delta=(f"{delta_prof:+.0f} vs corte de 743" if delta_prof is not None else None),
            help="Média ponderada pelo peso amostral. 743 pontos é o corte de "
                 "alfabetização definido pelo INEP.",
        )

    col_scatter, col_diag = st.columns([3, 2])

    with col_scatter:
        st.markdown("**Municípios: presença × taxa observada**")
        pa = q_presenca_aprendizagem(ano, rede, regiao, uf)
        if pa.empty:
            st.info("Sem microdados de aluno para o recorte selecionado.")
        else:
            fig = go.Figure()
            for status in ORDEM_STATUS:
                parte = pa[pa["status_meta"] == status]
                if parte.empty:
                    continue
                fig.add_trace(go.Scattergl(
                    x=parte["proporcao_presenca"],
                    y=parte["taxa_alfabetizacao_observada"],
                    mode="markers",
                    name=f"Meta {status.lower()}",
                    marker=dict(color=CORES_STATUS[status], size=7, opacity=0.45),
                    customdata=parte[["nome_municipio", "sigla_uf"]],
                    hovertemplate="%{customdata[0]} (%{customdata[1]})"
                                  "<br>Presença: %{x:.1f}%"
                                  "<br>Taxa observada: %{y:.1f}%<extra></extra>",
                ))
            fig.update_xaxes(title="Presença na prova (%)", ticksuffix="%")
            fig.update_yaxes(title="Taxa observada (%)", ticksuffix="%")
            st.plotly_chart(layout_base(fig, height=420), width="stretch")
            st.caption(
                "Cada ponto é um município; a cor indica o status da meta municipal. "
                "Pontos à esquerda faltaram mais — presença baixa pressiona o indicador."
            )

    with col_diag:
        st.markdown("**Diagnóstico dos municípios abaixo da meta**")
        diag = q_diagnostico_meta_presenca(ano, rede, regiao, uf)
        abaixo = diag[diag["status_meta"] == "Abaixo"]
        if abaixo.empty:
            st.info("Nenhum município abaixo da meta no recorte (ou ano sem meta).")
        else:
            presenca_ok = int(abaixo.loc[abaixo["status_presenca"] == "Atingida", "municipios"].sum())
            presenca_baixa = int(abaixo.loc[abaixo["status_presenca"] == "Abaixo", "municipios"].sum())
            sem_ref = int(abaixo.loc[~abaixo["status_presenca"].isin(["Atingida", "Abaixo"]), "municipios"].sum())
            rotulos = ["Aprendizagem (presença OK)", "Participação + aprendizagem", "Sem referência de presença"]
            valores = [presenca_ok, presenca_baixa, sem_ref]
            cores = [VERMELHO_STATUS, "#ec835a", CINZA_NEUTRO]
            fig = go.Figure(go.Bar(
                y=rotulos, x=valores,
                orientation="h",
                marker=dict(color=cores),
                text=[fmt_num(v) for v in valores],
                textposition="outside",
                hovertemplate="%{y}: %{x} municípios<extra></extra>",
            ))
            fig.update_xaxes(title="Municípios abaixo da meta",
                             range=[0, max(valores) * 1.2 if max(valores) else 1])
            st.plotly_chart(layout_base(fig, height=420), width="stretch")
            st.caption(
                "Presença OK e ainda abaixo da meta → o desafio é pedagógico. "
                "Presença abaixo da referência → parte do problema é participação."
            )
else:
    st.info(
        "**Métricas de aluno indisponíveis nesta versão da Gold.** "
        "Execute o fluxo de streaming e reprocesse Silver → Gold → crawler para "
        "habilitar a análise de presença × aprendizagem."
    )

st.divider()

# ----------------------------- Tabela detalhada ----------------------------
st.subheader("Tabela detalhada por município")
busca = st.text_input("Buscar município pelo nome", value="", placeholder="ex.: Campinas")

detalhe = q_tabela_detalhe(ano, rede, regiao, uf, busca)
if detalhe.empty:
    st.info("Nenhum município encontrado para o recorte/busca.")
else:
    st.dataframe(
        detalhe,
        width="stretch",
        hide_index=True,
        column_config={
            "id_municipio": st.column_config.TextColumn("Código IBGE"),
            "nome_municipio": st.column_config.TextColumn("Município"),
            "sigla_uf": st.column_config.TextColumn("UF"),
            "nome_regiao": st.column_config.TextColumn("Região"),
            "rede_nome": st.column_config.TextColumn("Rede"),
            "taxa_alfabetizacao_municipio": st.column_config.NumberColumn(
                "Taxa de alfabetização (%)", format="%.1f"),
            "meta_alfabetizacao_municipio": st.column_config.NumberColumn(
                "Meta do ano (%)", format="%.1f"),
            "meta_atingida_municipio": st.column_config.TextColumn("Status da meta"),
            "media_portugues_municipio": st.column_config.NumberColumn(
                "Média de Português (Saeb)", format="%.1f"),
            "percentual_participacao_municipio": st.column_config.NumberColumn(
                "Participação (%)", format="%.1f"),
            "taxa_alfabetizacao_uf": st.column_config.NumberColumn(
                "Taxa da UF (%)", format="%.1f"),
            "taxa_alfabetizacao_brasil_oficial": st.column_config.NumberColumn(
                "Taxa Brasil (%)", format="%.1f"),
        },
    )
    st.caption(
        f"{fmt_num(len(detalhe))} municípios exibidos (limite de 5.000 linhas), "
        "ordenados pela taxa de alfabetização."
    )
    st.download_button(
        "⬇️ Baixar recorte em CSV",
        detalhe.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"alfabetizacao_{ano}_rede{rede}.csv",
        mime="text/csv",
    )

st.caption(
    "Fonte: INEP — Avaliação de Alfabetização (camada Gold "
    f"`{GOLD_TABLE}`). A meta municipal refere-se à rede Municipal; metas de UF e "
    "Brasil, à rede pública. 2023 não possui metas definidas ('Sem meta')."
)
