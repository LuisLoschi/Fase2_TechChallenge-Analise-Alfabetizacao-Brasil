import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from datetime import datetime, timezone

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_NAME',
    'REGION'
])

BUCKET = args['BUCKET_NAME']
REGION = args['REGION']

BRONZE_PATH = f"s3://{BUCKET}/bronze/avaliacao_alfabetizacao_aluno"
STREAM_NAME = "stream-alfabetizacao-aluno"

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

schema_student = StructType([
    StructField("ano", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("id_escola", StringType(), True),
    StructField("id_aluno", StringType(), True),
    StructField("caderno", StringType(), True),
    StructField("serie", StringType(), True),
    StructField("rede", StringType(), True),
    StructField("presenca", StringType(), True),
    StructField("preenchimento_caderno", StringType(), True),
    StructField("alfabetizado", StringType(), True),
    StructField("proficiencia", StringType(), True),
    StructField("peso_aluno", StringType(), True),
])

# Lê do Kinesis como stream
kinesis_stream = spark \
    .readStream \
    .format("kinesis") \
    .option("streamName", STREAM_NAME) \
    .option("endpointUrl", f"https://kinesis.{REGION}.amazonaws.com") \
    .option("startingPosition", "TRIM_HORIZON") \
    .option("LabRole", "") \
    .load()


def process_batch(data_frame, batch_id):
    
    if data_frame.count() == 0:
        return

    # Kinesis envia a requisição na forma binária dentro do campo 'data', com isso foi necessário fazer a conversão para string
    # Define problemas ocorridos com as colunas nulas, foi necessário criar uma condição para que a conversão fosse possível dos campos mencionados abaixo
    df = data_frame.select(
        F.from_json(F.col("data").cast("string"), schema_student).alias("payload")
    ).select("payload.*") \
     .withColumn("preenchimento_caderno", F.when(F.col("preenchimento_caderno") == "", None).otherwise(F.col("preenchimento_caderno").cast(IntegerType()))) \
     .withColumn("alfabetizado", F.when(F.col("alfabetizado") == "", None).otherwise(F.col("alfabetizado").cast(IntegerType()))) \
     .withColumn("proficiencia", F.when(F.col("proficiencia") == "", None).otherwise(F.col("proficiencia").cast(DoubleType()))) \
     .withColumn("peso_aluno", F.when(F.col("peso_aluno") == "", None).otherwise(F.col("peso_aluno").cast(DoubleType())))

    now = datetime.now(timezone.utc)
    
    # Inserindo novos campos de controle da execução
    df = df \
        .withColumn("_ingestion_date", F.lit(now.strftime("%Y-%m-%d"))) \
        .withColumn("_ingestion_timestamp", F.lit(now.strftime("%Y%m%d_%H%M%S"))) \
        .withColumn("_source_entity", F.lit("stream-alfabetizacao-aluno"))

    df.write.mode("append").partitionBy("ano").parquet(BRONZE_PATH)
    

# Checkpoint no S3 armazena dados sobre a execução, caso algum erro aconteça, pode ser necessário apagar ele para que um novo seja criado
# Spark salva o progresso da execução nele
# Trigger está definido para coletar e processar os dados a cada 30 segundos
kinesis_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", f"s3://{BUCKET}/checkpoints/avaliacao_alfabetizacao_aluno/") \
    .trigger(processingTime="30 seconds") \
    .start() \
    .awaitTermination()

job.commit()