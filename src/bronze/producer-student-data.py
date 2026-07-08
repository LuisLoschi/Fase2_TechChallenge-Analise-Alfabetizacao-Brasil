import os
import csv
import time
import json
import boto3
import logging

BUCKET = os.environ["BUCKET_NAME"]
CSV_PATH = os.environ.get("CSV_PATH", "arquivos/br_inep_avaliacao_alfabetizacao_aluno.csv")
REGION = os.environ.get("AWS_REGION", "us-east-1")

STREAM_NAME = "stream-alfabetizacao-aluno"
TMP_PATH = "/tmp/aluno.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

log = logging.getLogger(__name__)

kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def download_csv():
    log.info(f"Baixando {CSV_PATH} para {TMP_PATH}...")
    s3.download_file(BUCKET, CSV_PATH, TMP_PATH)
    log.info(f"Download concluído: {os.path.getsize(TMP_PATH) / 1024 / 1024:.1f} MB")
    

# O delay entre lotes simula a chegada gradual de eventos
def send_records(batch_size=500, delay=0.3):

    with open(TMP_PATH, encoding="utf-8") as student_file:

        reader = csv.DictReader(student_file)
        batch  = []
        total  = 0

        for row in reader:

            batch.append({
                "Data": json.dumps(row, ensure_ascii=False),
                "PartitionKey": row.get("id_aluno", "default")
            })

            if len(batch) >= batch_size:

                kinesis.put_records(StreamName=STREAM_NAME, Records=batch)

                total += len(batch)

                log.info(f"Enviados: {total} registros")

                batch = []

                time.sleep(delay)

        if batch:

            kinesis.put_records(StreamName=STREAM_NAME, Records=batch)

            total += len(batch)

            log.info(f"Enviados: {total} registros")

    log.info(f"Concluído! Total enviado: {total}")


def lambda_handler(event, context):
    download_csv()
    send_records()
    return {"status": "ok"}
