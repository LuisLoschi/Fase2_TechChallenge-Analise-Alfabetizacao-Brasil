"""ETL da camada Silver — Avaliação de Alfabetização (INEP).

Job AWS Glue (Spark/PySpark) que consome as entidades da camada Bronze, aplica
padronização, tipagem, decodificação de domínios, deduplicação e enriquecimento
dimensional, e grava o resultado em Parquet na camada Silver, particionado por
`ano`.

Escopo da camada Silver: entregar dados limpos, tipados, padronizados e
rastreáveis, prontos para consumo analítico pela camada Gold. A Silver não
calcula indicadores, métricas nem agregações de negócio — essa responsabilidade
pertence à Gold.

Parâmetros do job:
    --JOB_NAME     Nome do job Glue (injetado pelo runtime).
    --BUCKET_NAME  Bucket S3 que contém os prefixos `bronze/` e `silver/`.

Entrada:  s3://<BUCKET_NAME>/bronze/<entidade>/
Saída:    s3://<BUCKET_NAME>/silver/<entidade>/  (Parquet particionado por `ano`)
"""

import sys
import logging
import unicodedata
from itertools import chain
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ---------------------------------------------------------------------------
# Parâmetros e inicialização do contexto Spark/Glue
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_NAME'
])

BUCKET = args['BUCKET_NAME']
BRONZE_PATH = f"s3://{BUCKET}/bronze/%s"
SILVER_PATH = f"s3://{BUCKET}/silver/%s"
# Prefixo das bases de apoio (diretórios oficiais de UF e município), usadas no
# enriquecimento dimensional. Ficam no mesmo prefixo dos arquivos de origem.
SUPPORT_PATH = f"s3://{BUCKET}/arquivos/%s"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Sobrescreve apenas as partições presentes em cada execução, preservando as demais.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------------------
# Contrato de processamento das entidades (Bronze -> Silver)
#
# Cada entidade declara seu contrato de transformação:
#   - integer_cols : colunas convertidas para inteiro.
#   - double_cols  : colunas convertidas para numérico de ponto flutuante.
#   - string_cols  : colunas mantidas como texto (ids preservam zeros à esquerda;
#                    `serie` e `rede` permanecem como código de origem).
#   - dedup_keys   : chave de negócio que identifica unicamente o grão da tabela.
#   - enrich       : dimensão de enriquecimento aplicável ("uf", "municipio" ou None).
#   - critical_cols: colunas monitoradas no relatório de qualidade (% de nulos).
#   - coalesce     : nº de arquivos por partição na escrita (None = mantém o
#                    paralelismo do Spark; valor inteiro consolida arquivos).
#
# Colunas com os prefixos `proporcao_aluno_nivel_` e `meta_alfabetizacao_` são
# convertidas dinamicamente para numérico, independentemente da entidade.
# ---------------------------------------------------------------------------
ENTITIES = {
    "avaliacao_alfabetizacao_uf": {
        "integer_cols": ["ano"],
        "double_cols": ["taxa_alfabetizacao", "media_portugues"],
        "string_cols": ["sigla_uf", "serie", "rede"],
        "dedup_keys": ["ano", "sigla_uf", "serie", "rede"],
        "enrich": "uf",
        "critical_cols": ["ano", "sigla_uf", "taxa_alfabetizacao", "media_portugues"],
        "coalesce": 1,
    },
    "avaliacao_alfabetizacao_municipio": {
        "integer_cols": ["ano"],
        "double_cols": ["taxa_alfabetizacao", "media_portugues"],
        "string_cols": ["id_municipio", "serie", "rede"],
        "dedup_keys": ["ano", "id_municipio", "serie", "rede"],
        "enrich": "municipio",
        "critical_cols": ["ano", "id_municipio", "taxa_alfabetizacao", "media_portugues"],
        "coalesce": 1,
    },
    "avaliacao_alfabetizacao_meta_alfabetizacao_brasil": {
        "integer_cols": ["ano"],
        "double_cols": ["taxa_alfabetizacao", "percentual_participacao"],
        "string_cols": ["rede"],
        "dedup_keys": ["ano", "rede"],
        "enrich": None,
        "critical_cols": ["ano", "taxa_alfabetizacao"],
        "coalesce": 1,
    },
    "avaliacao_alfabetizacao_meta_alfabetizacao_uf": {
        "integer_cols": ["ano"],
        "double_cols": ["taxa_alfabetizacao", "percentual_participacao"],
        "string_cols": ["sigla_uf", "rede"],
        "dedup_keys": ["ano", "sigla_uf", "rede"],
        "enrich": "uf",
        "critical_cols": ["ano", "sigla_uf", "taxa_alfabetizacao"],
        "coalesce": 1,
    },
    "avaliacao_alfabetizacao_meta_alfabetizacao_municipio": {
        "integer_cols": ["ano", "nivel_alfabetizacao"],
        "double_cols": ["taxa_alfabetizacao", "percentual_participacao"],
        "string_cols": ["id_municipio", "rede"],
        "dedup_keys": ["ano", "id_municipio", "rede"],
        "enrich": "municipio",
        "critical_cols": ["ano", "id_municipio", "taxa_alfabetizacao"],
        "coalesce": 1,
    },
    "avaliacao_alfabetizacao_aluno": {
        # Domínios dos campos: `serie` = '2'; `rede` ∈ {2,3,4}; `presenca` e
        # `alfabetizado` são indicadores 0/1 (inteiros, sem nulos); `proficiencia`
        # e `peso_aluno` são nulos apenas para alunos ausentes (`presenca` = 0).
        "integer_cols": ["ano", "alfabetizado", "presenca"],
        "double_cols": ["preenchimento_caderno", "proficiencia", "peso_aluno"],
        "string_cols": ["id_municipio", "id_escola", "id_aluno", "caderno", "serie", "rede"],
        "dedup_keys": ["ano", "id_aluno", "id_escola"],
        "enrich": "municipio",
        "critical_cols": ["ano", "id_aluno", "alfabetizado"],
        "coalesce": None,  # Tabela de alto volume: mantém o paralelismo de escrita do Spark.
    },
}


# ---------------------------------------------------------------------------
# Dicionários de domínio do INEP (Avaliação de Alfabetização)
#
# Tabelas de referência para traduzir os códigos de `rede` e `serie` em rótulos
# legíveis. Aplicam-se às entidades em que esses campos chegam como código
# (avaliação e aluno). Nas tabelas de meta, `rede` já é textual ('Pública' /
# 'Municipal') e o valor de origem é preservado.
#
# O mapeamento é unidirecional (código -> texto): 'Pública' nas metas não
# corresponde de forma única a um único código (5 ou 6), portanto a conversão
# inversa (texto -> código) não é realizada nesta camada.
# ---------------------------------------------------------------------------
REDE_MAP = {
    "0": "Total (Federal, Estadual, Municipal e Privada)",
    "1": "Federal",
    "2": "Estadual",
    "3": "Municipal",
    "4": "Privada",
    "5": "Pública (Estadual e Municipal)",
    "6": "Pública (Federal, Estadual e Municipal)",
}

SERIE_MAP = {
    "2": "2º ano do Ensino Fundamental",
}


# ---------------------------------------------------------------------------
# Funções de transformação (reutilizáveis entre entidades)
# ---------------------------------------------------------------------------
def bronze_path_exists(entity: str) -> bool:
    """Verifica se a pasta da entidade existe na camada Bronze.

    Usado para que entidades ainda não ingeridas (por exemplo, a tabela de aluno,
    quando o fluxo de streaming não foi executado) sejam puladas sem interromper
    o job.

    Args:
        entity: nome da pasta da entidade sob o prefixo `bronze/`.

    Returns:
        True se o caminho existir no S3; False caso contrário.
    """
    path = BRONZE_PATH % entity
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(hadoop_conf).exists(hadoop_path)


def read_bronze(entity: str) -> DataFrame:
    """Lê uma entidade da camada Bronze.

    Args:
        entity: nome da pasta da entidade sob o prefixo `bronze/`.

    Returns:
        DataFrame com o conteúdo Parquet da entidade (particionado por `ano`).
    """
    path = BRONZE_PATH % entity
    log.info(f"Lendo Bronze: {path}")
    df = spark.read.parquet(path)
    log.info(f"  -> {df.count()} registros | {len(df.columns)} colunas")
    return df


def _strip_accents(text: str) -> str:
    """Remove acentos e diacríticos de um texto, preservando a letra base."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Padroniza os nomes das colunas para `snake_case` em ASCII.

    Converte para minúsculas, remove acentos e substitui espaços e caracteres
    especiais por underscore, garantindo nomes consistentes entre as tabelas. O
    prefixo `_` das colunas técnicas de auditoria é preservado.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame com as colunas renomeadas.
    """
    renamed = {}
    for col in df.columns:
        is_technical = col.startswith("_")
        clean = _strip_accents(col).strip().lower()
        clean = "".join(ch if ch.isalnum() else "_" for ch in clean)
        while "__" in clean:
            clean = clean.replace("__", "_")
        clean = clean.strip("_")
        if is_technical:
            clean = "_" + clean
        renamed[col] = clean
    for old, new in renamed.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def empty_strings_to_null(df: DataFrame) -> DataFrame:
    """Normaliza as colunas de texto removendo espaços e tratando vazios.

    Aplica `trim` em todas as colunas de texto (exceto técnicas) e converte
    strings vazias em nulo, de modo que a ausência de informação seja
    representada de forma única (NULL) em toda a camada.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame com os textos normalizados.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType) and not field.name.startswith("_"):
            trimmed = F.trim(F.col(field.name))
            df = df.withColumn(
                field.name,
                F.when(trimmed == "", None).otherwise(trimmed),
            )
    return df


def cast_columns(df: DataFrame, config: dict) -> DataFrame:
    """Aplica o contrato de tipos da entidade.

    Converte as colunas para inteiro, numérico ou texto conforme declarado no
    contrato da entidade. Colunas de proporção e de meta são convertidas para
    numérico dinamicamente. Identificadores e códigos permanecem como texto para
    preservar zeros à esquerda.

    Args:
        df: DataFrame de entrada.
        config: contrato da entidade (chaves `integer_cols`, `double_cols`,
            `string_cols`).

    Returns:
        DataFrame com os tipos ajustados.
    """
    cols = set(df.columns)

    for col in config.get("integer_cols", []):
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(IntegerType()))

    for col in config.get("double_cols", []):
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Colunas numéricas por convenção de nome (proporções por nível e metas anuais).
    for col in df.columns:
        if col.startswith("proporcao_aluno_nivel_") or col.startswith("meta_alfabetizacao_"):
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Identificadores e códigos permanecem como texto (preserva zeros à esquerda).
    for col in config.get("string_cols", []):
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(StringType()))

    return df


def add_domain_labels(df: DataFrame) -> DataFrame:
    """Decodifica os códigos de domínio do INEP em rótulos legíveis.

    Cria as colunas `rede_nome` e `serie_nome` a partir de `REDE_MAP` e
    `SERIE_MAP`, preservando o código original para rastreabilidade. Valores que
    já chegam como texto (metas) não são encontrados no mapa e são mantidos pelo
    `coalesce`, garantindo que a coluna de rótulo nunca fique nula por engano.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame acrescido das colunas de rótulo, quando aplicável.
    """
    if "rede" in df.columns:
        rede_map = F.create_map([F.lit(v) for v in chain(*REDE_MAP.items())])
        df = df.withColumn("rede_nome", F.coalesce(rede_map[F.col("rede")], F.col("rede")))

    if "serie" in df.columns:
        serie_map = F.create_map([F.lit(v) for v in chain(*SERIE_MAP.items())])
        df = df.withColumn("serie_nome", F.coalesce(serie_map[F.col("serie")], F.col("serie")))

    return df


def deduplicate(df: DataFrame, keys: list, entity: str) -> DataFrame:
    """Remove registros duplicados pela chave de negócio da entidade.

    Mantém um único registro por combinação das colunas de chave e registra no
    log a diferença de volume, permitindo auditar quantos registros foram
    descartados.

    Args:
        df: DataFrame de entrada.
        keys: colunas que compõem a chave de negócio (grão da tabela).
        entity: nome da entidade, usado nas mensagens de log.

    Returns:
        DataFrame sem duplicidades na chave informada.
    """
    keys = [k for k in keys if k in df.columns]
    if not keys:
        log.warning(f"  [{entity}] Sem chave de deduplicação válida — nenhuma remoção aplicada.")
        return df
    before = df.count()
    df = df.dropDuplicates(keys)
    after = df.count()
    removed = before - after
    if removed > 0:
        log.warning(f"  [{entity}] Deduplicação por {keys}: {removed} registro(s) removido(s) "
                    f"({before} -> {after}).")
    else:
        log.info(f"  [{entity}] Sem duplicidades na chave {keys}.")
    return df


def add_technical_columns(df: DataFrame, entity: str) -> DataFrame:
    """Acrescenta as colunas técnicas de auditoria da camada Silver.

    Registra data e hora de processamento, a entidade de origem e os marcadores
    de camada e estágio do pipeline. As colunas técnicas herdadas da Bronze são
    preservadas, mantendo a rastreabilidade ponta a ponta.

    Args:
        df: DataFrame de entrada.
        entity: nome da entidade de origem.

    Returns:
        DataFrame acrescido das colunas técnicas.
    """
    now = datetime.now(timezone.utc)
    return df \
        .withColumn("_silver_processing_date", F.lit(now.strftime("%Y-%m-%d"))) \
        .withColumn("_silver_processing_timestamp", F.lit(now.strftime("%Y%m%d_%H%M%S"))) \
        .withColumn("_silver_source_entity", F.lit(entity)) \
        .withColumn("_source_layer", F.lit("bronze")) \
        .withColumn("_pipeline_stage", F.lit("silver"))


def data_quality_report(df: DataFrame, entity: str, critical_cols: list) -> None:
    """Registra no log um relatório de qualidade da entidade.

    Reporta o volume de registros, os anos presentes e o percentual de nulos nas
    colunas críticas. É um diagnóstico observável da execução; não cria
    indicadores nem altera os dados.

    Args:
        df: DataFrame já tratado.
        entity: nome da entidade, usado nas mensagens de log.
        critical_cols: colunas cuja completude deve ser monitorada.
    """
    total = df.count()
    log.info(f"  [DQ:{entity}] registros={total}")
    if "ano" in df.columns:
        anos = sorted(r["ano"] for r in df.select("ano").distinct().collect() if r["ano"] is not None)
        log.info(f"  [DQ:{entity}] anos={anos}")
    cols = [c for c in critical_cols if c in df.columns]
    if cols and total:
        nulls = df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols]).collect()[0]
        for c in cols:
            pct = 100 * nulls[c] / total
            level = log.warning if pct > 0 else log.info
            level(f"  [DQ:{entity}] nulos em '{c}': {nulls[c]} ({pct:.1f}%)")


def write_silver(df: DataFrame, entity: str, coalesce_n) -> None:
    """Persiste a entidade na camada Silver em Parquet.

    Grava no prefixo `silver/<entidade>/` particionando por `ano` quando a coluna
    existe. O parâmetro `coalesce_n` controla a quantidade de arquivos por
    partição, evitando a proliferação de arquivos pequenos em tabelas de baixo
    volume.

    Args:
        df: DataFrame final a ser gravado.
        entity: nome da entidade (define o caminho de destino).
        coalesce_n: número de arquivos por partição, ou None para manter o
            paralelismo do Spark.
    """
    path = SILVER_PATH % entity
    partition = ["ano"] if "ano" in df.columns else []
    if coalesce_n:
        df = df.coalesce(coalesce_n)
    log.info(f"Gravando Silver: {path} (partition={partition or 'nenhuma'}, coalesce={coalesce_n})")
    writer = df.write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(*partition)
    writer.parquet(path)
    log.info(f"  -> gravação concluída em {path}")


# ---------------------------------------------------------------------------
# Dimensões de apoio e enriquecimento
# ---------------------------------------------------------------------------
def load_dim_uf() -> DataFrame:
    """Carrega a dimensão de UF a partir do diretório oficial de UFs.

    Returns:
        DataFrame com as colunas `sigla_uf`, `nome_uf` e `nome_regiao`, com uma
        linha por UF.
    """
    path = SUPPORT_PATH % "br_bd_diretorios_brasil_uf.csv"
    df = spark.read.option("header", "true").option("delimiter", ",").csv(path)
    return df.select(
        F.trim(F.col("sigla")).alias("sigla_uf"),
        F.trim(F.col("nome")).alias("nome_uf"),
        F.trim(F.col("regiao")).alias("nome_regiao"),
    ).dropDuplicates(["sigla_uf"])


def load_dim_municipio() -> DataFrame:
    """Carrega a dimensão de município a partir do diretório oficial de municípios.

    Returns:
        DataFrame com as colunas `id_municipio`, `nome_municipio`, `sigla_uf`,
        `nome_uf` e `nome_regiao`, com uma linha por município.
    """
    path = SUPPORT_PATH % "br_bd_diretorios_brasil_municipio.csv"
    df = spark.read.option("header", "true").option("delimiter", ",").csv(path)
    return df.select(
        F.trim(F.col("id_municipio")).cast(StringType()).alias("id_municipio"),
        F.trim(F.col("nome")).alias("nome_municipio"),
        F.trim(F.col("sigla_uf")).alias("sigla_uf"),
        F.trim(F.col("nome_uf")).alias("nome_uf"),
        F.trim(F.col("nome_regiao")).alias("nome_regiao"),
    ).dropDuplicates(["id_municipio"])


def enrich(df: DataFrame, kind: str, dims: dict) -> DataFrame:
    """Enriquece a entidade com atributos descritivos de UF ou município.

    Faz um `left join` com a dimensão correspondente para adicionar nomes de UF,
    município e região. O `broadcast` é adequado por serem dimensões pequenas. O
    join apenas acrescenta colunas descritivas e não altera o grão da entidade.

    Args:
        df: DataFrame da entidade.
        kind: dimensão a aplicar ("uf" ou "municipio").
        dims: dimensões pré-carregadas (chaves "uf" e "municipio").

    Returns:
        DataFrame enriquecido, ou o DataFrame original quando não há chave de join.
    """
    if kind == "uf" and "sigla_uf" in df.columns:
        return df.join(F.broadcast(dims["uf"]), on="sigla_uf", how="left")
    if kind == "municipio" and "id_municipio" in df.columns:
        return df.join(F.broadcast(dims["municipio"]), on="id_municipio", how="left")
    return df


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def process_entity(entity: str, config: dict, dims: dict) -> None:
    """Executa o fluxo Bronze -> Silver de uma entidade.

    Ordem das etapas:
        1. Leitura da camada Bronze.
        2. Padronização dos nomes de colunas.
        3. Normalização de texto (trim e vazio -> nulo).
        4. Tipagem conforme o contrato da entidade.
        5. Decodificação de domínios (`rede` e `serie`).
        6. Deduplicação pela chave de negócio.
        7. Enriquecimento dimensional (UF/município), quando aplicável.
        8. Inclusão das colunas técnicas de auditoria.
        9. Relatório de qualidade e gravação na Silver.

    Args:
        entity: nome da entidade a processar.
        config: contrato de transformação da entidade.
        dims: dimensões de apoio pré-carregadas.
    """
    log.info("=" * 70)
    log.info(f"Processando entidade: {entity}")

    df = read_bronze(entity)
    df = standardize_column_names(df)
    df = empty_strings_to_null(df)
    df = cast_columns(df, config)
    df = add_domain_labels(df)
    df = deduplicate(df, config["dedup_keys"], entity)

    if config.get("enrich"):
        df = enrich(df, config["enrich"], dims)

    df = add_technical_columns(df, entity)
    # Materializa o resultado uma vez, pois ele é reutilizado pelo relatório e pela escrita.
    df = df.cache()

    data_quality_report(df, entity, config.get("critical_cols", []))
    write_silver(df, entity, config.get("coalesce"))
    df.unpersist()


def main() -> None:
    """Ponto de entrada do job: carrega as dimensões e processa cada entidade.

    Interrompe a execução na primeira falha de entidade, garantindo que a Silver
    não seja gravada parcialmente sem registro do erro.
    """
    log.info("Iniciando ETL da camada Silver — Alfabetização INEP")

    dims = {
        "uf": load_dim_uf(),
        "municipio": load_dim_municipio(),
    }
    log.info(f"Dimensões carregadas: UF={dims['uf'].count()} | "
             f"Município={dims['municipio'].count()}")

    for entity, config in ENTITIES.items():
        if not bronze_path_exists(entity):
            log.warning(f"Entidade ausente na Bronze — pulando: {entity}")
            continue
        try:
            process_entity(entity, config, dims)
        except Exception as e:
            log.error(f"Falha ao processar {entity}: {e}")
            raise

    log.info("=" * 70)
    log.info("ETL Silver concluído com sucesso.")
    job.commit()


main()
