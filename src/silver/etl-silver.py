"""ETL da camada Silver — Avaliação de Alfabetização (INEP).

Consome as entidades da Bronze, aplica padronização, tipagem, decodificação de
domínios, deduplicação e enriquecimento dimensional, e grava em Parquet
particionado por `ano` em s3://<BUCKET_NAME>/silver/<entidade>/. A Silver não
calcula indicadores nem agregações de negócio — isso pertence à Gold.
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
# Bases de apoio (diretórios de UF e município), no mesmo prefixo dos arquivos de origem
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
# Contrato de transformação por entidade: tipos, chave de deduplicação (grão),
# dimensão de enriquecimento, colunas monitoradas e nº de arquivos na escrita.
# Colunas `proporcao_aluno_nivel_*` e `meta_alfabetizacao_*` viram double em
# qualquer entidade.
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
# Domínios do INEP: código de `rede`/`serie` -> rótulo. Nas tabelas de meta,
# `rede` já é texto ('Pública'/'Municipal') e é preservada como está —
# 'Pública' não mapeia de forma única para os códigos 5 ou 6.
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
    """Verifica se a entidade existe na Bronze (entidades não ingeridas são puladas)."""
    path = BRONZE_PATH % entity
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(hadoop_conf).exists(hadoop_path)


def read_bronze(entity: str) -> DataFrame:
    path = BRONZE_PATH % entity
    log.info(f"Lendo Bronze: {path}")
    df = spark.read.parquet(path)
    log.info(f"  -> {df.count()} registros | {len(df.columns)} colunas")
    return df


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Padroniza nomes de colunas para snake_case ASCII, preservando o prefixo `_` técnico."""
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
    """Aplica trim nas colunas de texto e converte string vazia em NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType) and not field.name.startswith("_"):
            trimmed = F.trim(F.col(field.name))
            df = df.withColumn(
                field.name,
                F.when(trimmed == "", None).otherwise(trimmed),
            )
    return df


def cast_columns(df: DataFrame, config: dict) -> DataFrame:
    """Aplica o contrato de tipos; ids ficam como texto para preservar zeros à esquerda."""
    cols = set(df.columns)

    for col in config.get("integer_cols", []):
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(IntegerType()))

    for col in config.get("double_cols", []):
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    for col in df.columns:
        if col.startswith("proporcao_aluno_nivel_") or col.startswith("meta_alfabetizacao_"):
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    for col in config.get("string_cols", []):
        if col in cols:
            df = df.withColumn(col, F.col(col).cast(StringType()))

    return df


def add_domain_labels(df: DataFrame) -> DataFrame:
    """Cria `rede_nome`/`serie_nome`; valores já textuais (metas) são mantidos pelo coalesce."""
    if "rede" in df.columns:
        rede_map = F.create_map([F.lit(v) for v in chain(*REDE_MAP.items())])
        df = df.withColumn("rede_nome", F.coalesce(rede_map[F.col("rede")], F.col("rede")))

    if "serie" in df.columns:
        serie_map = F.create_map([F.lit(v) for v in chain(*SERIE_MAP.items())])
        df = df.withColumn("serie_nome", F.coalesce(serie_map[F.col("serie")], F.col("serie")))

    return df


def deduplicate(df: DataFrame, keys: list, entity: str) -> DataFrame:
    """Remove duplicidades pela chave de negócio, logando o volume descartado."""
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
    """Adiciona colunas de auditoria da Silver (as herdadas da Bronze são preservadas)."""
    now = datetime.now(timezone.utc)
    return df \
        .withColumn("_silver_processing_date", F.lit(now.strftime("%Y-%m-%d"))) \
        .withColumn("_silver_processing_timestamp", F.lit(now.strftime("%Y%m%d_%H%M%S"))) \
        .withColumn("_silver_source_entity", F.lit(entity)) \
        .withColumn("_source_layer", F.lit("bronze")) \
        .withColumn("_pipeline_stage", F.lit("silver"))


def data_quality_report(df: DataFrame, entity: str, critical_cols: list) -> None:
    """Loga volume, anos presentes e % de nulos nas colunas críticas."""
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
    """Grava em silver/<entidade>/ particionado por `ano`; coalesce_n limita arquivos por partição."""
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
    """Dimensão de UF (uma linha por sigla) a partir do diretório oficial."""
    path = SUPPORT_PATH % "br_bd_diretorios_brasil_uf.csv"
    df = spark.read.option("header", "true").option("delimiter", ",").csv(path)
    return df.select(
        F.trim(F.col("sigla")).alias("sigla_uf"),
        F.trim(F.col("nome")).alias("nome_uf"),
        F.trim(F.col("regiao")).alias("nome_regiao"),
    ).dropDuplicates(["sigla_uf"])


def load_dim_municipio() -> DataFrame:
    """Dimensão de município (uma linha por código IBGE) a partir do diretório oficial."""
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
    """Left join com a dimensão de UF ou município; apenas acrescenta colunas descritivas."""
    if kind == "uf" and "sigla_uf" in df.columns:
        return df.join(F.broadcast(dims["uf"]), on="sigla_uf", how="left")
    if kind == "municipio" and "id_municipio" in df.columns:
        return df.join(F.broadcast(dims["municipio"]), on="id_municipio", how="left")
    return df


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def process_entity(entity: str, config: dict, dims: dict) -> None:
    """Executa o fluxo Bronze -> Silver de uma entidade."""
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
    # Reutilizado pelo relatório de qualidade e pela escrita
    df = df.cache()

    data_quality_report(df, entity, config.get("critical_cols", []))
    write_silver(df, entity, config.get("coalesce"))
    df.unpersist()


def main() -> None:
    """Carrega as dimensões e processa cada entidade; interrompe na primeira falha."""
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
