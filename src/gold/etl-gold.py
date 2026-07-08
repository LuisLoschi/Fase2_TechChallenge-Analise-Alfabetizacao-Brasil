"""ETL da camada Gold — consolida a Silver em uma tabela analítica única.

Grão: ano x município x série x rede. A meta comparada é sempre a do ano da
linha (coluna `meta_alfabetizacao_<ano>`); anos sem meta definida (ex.: 2023)
recebem o status 'Sem meta'.
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_NAME'
])

BUCKET = args['BUCKET_NAME']
SILVER_PATH = f"s3://{BUCKET}/silver/%s"
GOLD_PATH = f"s3://{BUCKET}/gold/%s"

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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")


GOLD_TABLE = "alfabetizacao_analise_output"
SILVER_ENTITIES = [
    "avaliacao_alfabetizacao_uf",
    "avaliacao_alfabetizacao_municipio",
    "avaliacao_alfabetizacao_meta_alfabetizacao_brasil",
    "avaliacao_alfabetizacao_meta_alfabetizacao_uf",
    "avaliacao_alfabetizacao_meta_alfabetizacao_municipio",
    "avaliacao_alfabetizacao_aluno",
]
REQUIRED_ENTITIES = [
    "avaliacao_alfabetizacao_municipio",
    "avaliacao_alfabetizacao_uf",
]


def s3_path_exists(path: str) -> bool:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(hadoop_conf)
    return fs.exists(hadoop_path)


def read_silver(entity: str) -> DataFrame:
    path = SILVER_PATH % entity

    log.info("=" * 70)
    log.info(f"Tabela Silver carregada: {entity}")
    log.info(f"Caminho: {path}")
    log.info("=" * 70)

    df = spark.read.parquet(path)

    log.info(f"Total lido -> {df.count()} registros | {len(df.columns)} colunas")
    log.info("=" * 70)

    return df


def load_silver_tables() -> dict:
    tables = {}
    for entity in SILVER_ENTITIES:
        path = SILVER_PATH % entity
        if s3_path_exists(path):
            tables[entity] = read_silver(entity)
        else:
            log.warning(f"Caminho Silver ausente: {path} — essa tabela será ignorada.")

    log.info("=" * 70)
    log.info(f"Total de tabelas Silver carregadas: {len(tables)} / {len(SILVER_ENTITIES)}")
    log.info("=" * 70)

    return tables


def aggregate_student_metrics(aluno_df: DataFrame) -> DataFrame:
    """Agrega os indicadores de aluno por (ano, município, série, rede).

    Além dos códigos presentes no dado de aluno (2, 3, 4), deriva as redes
    compostas usadas pelas tabelas de avaliação: 5 = Pública (2+3) e
    0 = Total. Taxa observada e proficiência consideram apenas os presentes.
    """
    metrics = [
        F.count(F.lit(1)).alias("alunos_total"),
        F.sum("presenca").alias("alunos_presentes"),
        F.sum("alfabetizado").alias("alunos_alfabetizados"),
        F.avg("proficiencia").alias("_proficiencia_media"),
        F.sum(F.col("proficiencia") * F.col("peso_aluno")).alias("_proficiencia_x_peso"),
        F.sum(F.when(F.col("proficiencia").isNotNull(), F.col("peso_aluno"))).alias("_peso_presentes"),
    ]

    por_rede = aluno_df.groupBy("ano", "id_municipio", "serie", "rede").agg(*metrics)
    rede_publica = (
        aluno_df.filter(F.col("rede").isin("2", "3"))
        .groupBy("ano", "id_municipio", "serie").agg(*metrics)
        .withColumn("rede", F.lit("5"))
    )
    rede_total = (
        aluno_df.groupBy("ano", "id_municipio", "serie").agg(*metrics)
        .withColumn("rede", F.lit("0"))
    )
    aggregates = por_rede.unionByName(rede_publica).unionByName(rede_total)

    return (
        aggregates
        .withColumn(
            "proporcao_presenca",
            F.round(F.col("alunos_presentes") / F.col("alunos_total") * 100, 2),
        )
        .withColumn(
            "taxa_alfabetizacao_observada",
            F.when(
                F.col("alunos_presentes") > 0,
                F.round(F.col("alunos_alfabetizados") / F.col("alunos_presentes") * 100, 2),
            ),
        )
        .withColumn("proficiencia_media", F.round(F.col("_proficiencia_media"), 2))
        .withColumn(
            "proficiencia_media_ponderada",
            F.when(
                F.col("_peso_presentes") > 0,
                F.round(F.col("_proficiencia_x_peso") / F.col("_peso_presentes"), 2),
            ),
        )
        .drop("_proficiencia_media", "_proficiencia_x_peso", "_peso_presentes")
    )


def meta_do_ano(df: DataFrame) -> Column:
    """Retorna a meta anual do ano da linha (`meta_alfabetizacao_<ano>`); nulo se não houver."""
    meta_cols = sorted(
        c for c in df.columns
        if c.startswith("meta_alfabetizacao_") and c.rsplit("_", 1)[-1].isdigit()
    )
    expr = F.lit(None).cast(DoubleType())
    for col in meta_cols:
        year = int(col.rsplit("_", 1)[-1])
        expr = F.when(F.col("ano") == year, F.col(col)).otherwise(expr)
    return expr


def add_goal_flag(df: DataFrame, flag_col: str, value_col: str, target_col: str) -> DataFrame:
    """Flag 'Atingida' | 'Abaixo' | 'Sem meta' | 'Sem dado'; não é criada se as colunas não existem."""
    if value_col not in df.columns or target_col not in df.columns:
        return df
    return df.withColumn(
        flag_col,
        F.when(F.col(target_col).isNull(), F.lit("Sem meta"))
         .when(F.col(value_col).isNull(), F.lit("Sem dado"))
         .when(F.col(value_col) >= F.col(target_col), F.lit("Atingida"))
         .otherwise(F.lit("Abaixo")),
    )


def unify_gold_table(tables: dict) -> DataFrame:
    avaliacao_municipio_df = tables["avaliacao_alfabetizacao_municipio"]
    avaliacao_uf_df        = tables["avaliacao_alfabetizacao_uf"]
    aluno_df               = tables.get("avaliacao_alfabetizacao_aluno")
    meta_municipio_df      = tables.get("avaliacao_alfabetizacao_meta_alfabetizacao_municipio")
    meta_uf_df             = tables.get("avaliacao_alfabetizacao_meta_alfabetizacao_uf")
    meta_brasil_df         = tables.get("avaliacao_alfabetizacao_meta_alfabetizacao_brasil")

    base = avaliacao_municipio_df.select(
        "ano",
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "nome_regiao",
        "serie",
        "serie_nome",
        "rede",
        "rede_nome",
        "taxa_alfabetizacao",
        "media_portugues",
        ).withColumnRenamed("taxa_alfabetizacao", "taxa_alfabetizacao_municipio"
        ).withColumnRenamed("media_portugues"   , "media_portugues_municipio"
    )

    if aluno_df is not None:
        base = base.join(
            aggregate_student_metrics(aluno_df),
            on=["ano", "id_municipio", "serie", "rede"],
            how="left",
        )
    else:
        log.warning("Silver de aluno ausente — métricas de aluno não serão calculadas.")

    uf_avaliacao = avaliacao_uf_df.select(
        "ano",
        "sigla_uf",
        "serie",
        "rede",
        "taxa_alfabetizacao",
        "media_portugues",
        ).withColumnRenamed("taxa_alfabetizacao", "taxa_alfabetizacao_uf"
        ).withColumnRenamed("media_portugues"   , "media_portugues_uf"
    )
    base = base.join(
        uf_avaliacao,
        on=["ano", "sigla_uf", "serie", "rede"],
        how="left",
    )

    # Visão Brasil: média simples das UFs (fontes não têm pesos por UF);
    # a taxa nacional oficial vem da tabela de meta do Brasil, mais abaixo.
    brasil_avaliacao = avaliacao_uf_df.groupBy("ano", "serie", "rede").agg(
        F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_brasil"),
        F.round(F.avg("media_portugues"), 2).alias("media_portugues_brasil"),
    )
    base = base.join(
        brasil_avaliacao,
        on=["ano", "serie", "rede"],
        how="left",
    )

    if meta_municipio_df is not None:
        meta_municipio = meta_municipio_df.select(
            "ano",
            "id_municipio",
            meta_do_ano(meta_municipio_df).alias("meta_alfabetizacao_municipio"),
            F.col("percentual_participacao").alias("percentual_participacao_municipio"),
        )
        base = base.join(meta_municipio, on=["ano", "id_municipio"], how="left")

    if meta_uf_df is not None:
        meta_uf = meta_uf_df.select(
            "ano",
            "sigla_uf",
            meta_do_ano(meta_uf_df).alias("meta_alfabetizacao_uf"),
            F.col("percentual_participacao").alias("percentual_participacao_uf"),
        )
        base = base.join(meta_uf, on=["ano", "sigla_uf"], how="left")

    if meta_brasil_df is not None:
        meta_brasil = meta_brasil_df.select(
            "ano",
            meta_do_ano(meta_brasil_df).alias("meta_alfabetizacao_brasil"),
            F.col("taxa_alfabetizacao").alias("taxa_alfabetizacao_brasil_oficial"),
            F.col("percentual_participacao").alias("percentual_participacao_brasil"),
        )
        base = base.join(meta_brasil, on=["ano"], how="left")

    base = add_goal_flag(base, "meta_atingida_municipio", "taxa_alfabetizacao_municipio", "meta_alfabetizacao_municipio")
    base = add_goal_flag(base, "meta_atingida_uf", "taxa_alfabetizacao_uf", "meta_alfabetizacao_uf")
    base = add_goal_flag(base, "meta_atingida_brasil", "taxa_alfabetizacao_brasil_oficial", "meta_alfabetizacao_brasil")

    base = add_goal_flag(base, "meta_atingida_presenca_municipio", "proporcao_presenca", "percentual_participacao_municipio")
    base = add_goal_flag(base, "meta_atingida_presenca_uf", "proporcao_presenca", "percentual_participacao_uf")
    base = add_goal_flag(base, "meta_atingida_presenca_brasil", "proporcao_presenca", "percentual_participacao_brasil")

    selectable_columns = [
        "ano",
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "nome_regiao",
        "serie_nome",
        "rede",
        "rede_nome",
        "alunos_total",
        "alunos_presentes",
        "alunos_alfabetizados",
        "proporcao_presenca",
        "taxa_alfabetizacao_observada",
        "proficiencia_media",
        "proficiencia_media_ponderada",
        "taxa_alfabetizacao_municipio",
        "media_portugues_municipio",
        "meta_alfabetizacao_municipio",
        "percentual_participacao_municipio",
        "meta_atingida_municipio",
        "meta_atingida_presenca_municipio",
        "taxa_alfabetizacao_uf",
        "media_portugues_uf",
        "meta_alfabetizacao_uf",
        "percentual_participacao_uf",
        "meta_atingida_uf",
        "meta_atingida_presenca_uf",
        "taxa_alfabetizacao_brasil",
        "media_portugues_brasil",
        "taxa_alfabetizacao_brasil_oficial",
        "meta_alfabetizacao_brasil",
        "percentual_participacao_brasil",
        "meta_atingida_brasil",
        "meta_atingida_presenca_brasil",
    ]

    existing = [c for c in selectable_columns if c in base.columns]
    return base.select(*existing)


def write_gold(df: DataFrame, entity: str) -> None:
    path = GOLD_PATH % entity
    partition_cols = ["ano"] if "ano" in df.columns else []

    rows = df.count()
    cols = len(df.columns)

    log.info("=" * 70)
    log.info(f"Gravando Gold: {entity}")
    log.info(f"Destino: {path}")
    log.info(f"shape final Gold=({rows}, {cols})")
    log.info(f"partition={partition_cols or 'nenhuma'}")
    log.info("=" * 70)

    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)

    log.info("=" * 70)
    log.info(f"  -> Gold gravado em {path}")
    log.info("=" * 70)


def main() -> None:
    log.info("=" * 70)
    log.info("Iniciando ETL da camada Gold — Métricas de Alfabetização")
    log.info(f"Bucket: {BUCKET}")
    log.info("=" * 70)

    silver_tables = load_silver_tables()

    missing = [e for e in REQUIRED_ENTITIES if e not in silver_tables]
    if missing:
        raise RuntimeError(
            f"Tabelas Silver obrigatórias ausentes: {missing}. "
            "Execute o ETL Silver antes do ETL Gold."
        )

    now = datetime.now(timezone.utc)
    gold_df = unify_gold_table(silver_tables)
    gold_df = gold_df.withColumn("_gold_processing_date", F.lit(now.strftime("%Y-%m-%d")))
    gold_df = gold_df.withColumn("_gold_processing_timestamp", F.lit(now.strftime("%Y%m%d_%H%M%S")))
    gold_df = gold_df.withColumn("_gold_source_bucket", F.lit(BUCKET))
    gold_df = gold_df.withColumn("_pipeline_stage", F.lit("gold"))

    write_gold(gold_df, GOLD_TABLE)

    log.info("=" * 70)
    log.info("ETL Gold concluído com sucesso.")
    log.info("=" * 70)
    job.commit()

if __name__ == "__main__":
    main()
