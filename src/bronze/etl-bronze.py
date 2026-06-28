import os
import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_NAME'
])

# Definindo variáveis principais
BUCKET = args['BUCKET_NAME']
S3_PATH = f"s3://{BUCKET}/arquivos"
BRONZE_PATH = f"s3://{BUCKET}/bronze/%s"

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


def get_files():
    return {
        "br_inep_avaliacao_alfabetizacao_uf.csv": define_struct_type_evaluation_state_literacy(),
        "br_inep_avaliacao_alfabetizacao_municipio.csv": define_struct_type_evaluation_city_literacy(),
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv": define_struct_type_evaluation_country_literacy_goal(),
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv": define_struct_type_evaluation_state_literacy_goal(),
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv": define_struct_type_evaluation_city_literacy_goal(),
    }


# Estrutura do arquivo br_inep_avaliacao_alfabetizacao_uf.csv
def define_struct_type_evaluation_state_literacy():
    return StructType([
        StructField("ano", IntegerType(), False),
        StructField("sigla_uf", StringType(), False),
        StructField("serie", StringType(), False),
        StructField("rede", StringType(), False),
        StructField("taxa_alfabetizacao", DoubleType(), False),
        StructField("media_portugues", DoubleType(), False),
        StructField("proporcao_aluno_nivel_0", DoubleType(), True),
        StructField("proporcao_aluno_nivel_1", DoubleType(), True),
        StructField("proporcao_aluno_nivel_2", DoubleType(), True),
        StructField("proporcao_aluno_nivel_3", DoubleType(), True),
        StructField("proporcao_aluno_nivel_4", DoubleType(), True),
        StructField("proporcao_aluno_nivel_5", DoubleType(), True),
        StructField("proporcao_aluno_nivel_6", DoubleType(), True),
        StructField("proporcao_aluno_nivel_7", DoubleType(), True),
        StructField("proporcao_aluno_nivel_8", DoubleType(), True)
    ])


# Estrutura do arquivo br_inep_avaliacao_alfabetizacao_municipio.csv
def define_struct_type_evaluation_city_literacy():
    return StructType([
        StructField("ano", IntegerType(), False),
        StructField("id_municipio", StringType(), False),
        StructField("serie", StringType(), False),
        StructField("rede", StringType(), False),
        StructField("taxa_alfabetizacao", DoubleType(), False),
        StructField("media_portugues", DoubleType(), False),
        StructField("proporcao_aluno_nivel_0", DoubleType(), True),
        StructField("proporcao_aluno_nivel_1", DoubleType(), True),
        StructField("proporcao_aluno_nivel_2", DoubleType(), True),
        StructField("proporcao_aluno_nivel_3", DoubleType(), True),
        StructField("proporcao_aluno_nivel_4", DoubleType(), True),
        StructField("proporcao_aluno_nivel_5", DoubleType(), True),
        StructField("proporcao_aluno_nivel_6", DoubleType(), True),
        StructField("proporcao_aluno_nivel_7", DoubleType(), True),
        StructField("proporcao_aluno_nivel_8", DoubleType(), True)
    ])


# Estrutura do arquivo br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv
def define_struct_type_evaluation_country_literacy_goal():
    return StructType([
        StructField("ano", IntegerType(), True),
        StructField("rede", StringType(), True),
        StructField("taxa_alfabetizacao", DoubleType(), True),
        StructField("meta_alfabetizacao_2024", DoubleType(), True),
        StructField("meta_alfabetizacao_2025", DoubleType(), True),
        StructField("meta_alfabetizacao_2026", DoubleType(), True),
        StructField("meta_alfabetizacao_2027", DoubleType(), True),
        StructField("meta_alfabetizacao_2028", DoubleType(), True),
        StructField("meta_alfabetizacao_2029", DoubleType(), True),
        StructField("meta_alfabetizacao_2030", DoubleType(), True),
        StructField("percentual_participacao", DoubleType(), True)
    ])


# Estrutura do arquivo br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv
def define_struct_type_evaluation_state_literacy_goal():
    return StructType([
        StructField("ano", IntegerType(), True),
        StructField("sigla_uf", StringType(), True),
        StructField("rede", StringType(), True),
        StructField("taxa_alfabetizacao", DoubleType(), True),
        StructField("meta_alfabetizacao_2024", DoubleType(), True),
        StructField("meta_alfabetizacao_2025", DoubleType(), True),
        StructField("meta_alfabetizacao_2026", DoubleType(), True),
        StructField("meta_alfabetizacao_2027", DoubleType(), True),
        StructField("meta_alfabetizacao_2028", DoubleType(), True),
        StructField("meta_alfabetizacao_2029", DoubleType(), True),
        StructField("meta_alfabetizacao_2030", DoubleType(), True),
        StructField("percentual_participacao", DoubleType(), True)
    ])


# Estrutura do arquivo br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv
def define_struct_type_evaluation_city_literacy_goal():
    return StructType([
        StructField("ano", IntegerType(), False),
        StructField("id_municipio", StringType(), False),
        StructField("rede", StringType(), False),
        StructField("taxa_alfabetizacao", DoubleType(), True),
        StructField("meta_alfabetizacao_2024", DoubleType(), True),
        StructField("meta_alfabetizacao_2025", DoubleType(), True),
        StructField("meta_alfabetizacao_2026", DoubleType(), True),
        StructField("meta_alfabetizacao_2027", DoubleType(), True),
        StructField("meta_alfabetizacao_2028", DoubleType(), True),
        StructField("meta_alfabetizacao_2029", DoubleType(), True),
        StructField("meta_alfabetizacao_2030", DoubleType(), True),
        StructField("nivel_alfabetizacao", IntegerType(), True),
        StructField("percentual_participacao", DoubleType(), True)
    ])


def read_file(filename, schema):
    
    log.info(f"Lendo arquivo: {filename}")
    
    try:
        path = f"{S3_PATH}/{filename}"
        print(f"[DEBUG] Path: {path}")
        
        df = spark.read \
          .option("header", "true") \
          .option("delimiter", ',') \
          .option("encoding", 'UTF-8') \
          .option("mode", "PERMISSIVE") \
          .option("nullValue", "") \
          .option("emptyValue", "") \
          .schema(schema) \
          .csv(f"{S3_PATH}/{filename}")
        
        total = df.count()
        
        print(f"[DEBUG] Total lido: {total} registros | colunas={len(df.columns)}")
        log.info(f"Foram lidos {total} registros do arquivo {filename} | colunas={len(df.columns)}")
        
        print(f"[DEBUG] Primeiras linhas de {filename}:")
        for row in df.take(3):
            print(f"[DEBUG] {row}")
        
        df = add_audit_info(filename, df)
        
        return df
    
    except Exception as e:
        log.error(f"Falha ao ler arquivo {filename}: {e}")
        print(f"[ERROR] Falha ao ler {filename}: {e}")
        raise


# Adicionando colunas de ingestão
def add_audit_info(filename, df):
    
    log.info("Adicionando metadados de ingestao")
    
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = S3_PATH + filename
    
    return df \
        .withColumn("_ingestion_date",      F.lit(ingestion_date)) \
        .withColumn("_ingestion_timestamp", F.lit(ingestion_timestamp)) \
        .withColumn("_source_path",         F.lit(path)) \
        .withColumn("_source_entity",       F.lit(filename))
    

# Criando a camada bronze    
def save_bronze_layer_data(filename, df):
    
    folder_name = normalize_folder_name(filename)   # <- aqui
    file_path = BRONZE_PATH % folder_name
    
    log.info(f"Salvando em: {file_path}")
    print(f"[DEBUG] Salvando em: {file_path}")
    
    df.write \
      .mode("overwrite") \
      .partitionBy("ano") \
      .parquet(file_path)
      
    print(f"[DEBUG] Gravação concluída em: {file_path}")
    log.info(f"Foram gravados {df.count()} registros")


def normalize_folder_name(filename):
    return filename.replace("br_inep_", "").replace(".csv", "")


# Iniciando processamento
log.info("Iniciando a leitura dos arquivos do Inep")

for filename, schema in get_files().items():
    df = read_file(filename, schema)
    save_bronze_layer_data(filename, df)
    log.info("=" * 65)
