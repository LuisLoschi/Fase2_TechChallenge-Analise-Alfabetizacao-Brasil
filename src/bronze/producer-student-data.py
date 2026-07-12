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

BATCH_SIZE = 500

DELAY = float(os.environ.get("SEND_DELAY", "0.05"))

MAX_RETRIES = 5

SAFETY_MARGIN_MS = 60_000
LOG_EVERY = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

log = logging.getLogger(__name__)

kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)


def download_csv():
    log.info(f"Baixando {CSV_PATH} para {TMP_PATH}...")
    s3.download_file(BUCKET, CSV_PATH, TMP_PATH)
    log.info(f"Download concluído: {os.path.getsize(TMP_PATH) / 1024 / 1024:.1f} MB")


def put_with_retry(batch):
    """Envia um lote tratando falhas parciais.

    O PutRecords não é atômico: a resposta traz FailedRecordCount e o erro de
    cada registro rejeitado. Reenvia apenas os rejeitados, com backoff
    exponencial.
    """
    pending = batch

    for attempt in range(MAX_RETRIES + 1):

        response = kinesis.put_records(StreamName=STREAM_NAME, Records=pending)

        failed = response.get("FailedRecordCount", 0)
        if failed == 0:
            return

        pending = [
            record
            for record, result in zip(pending, response["Records"])
            if "ErrorCode" in result
        ]

        wait = min(0.2 * (2 ** attempt), 5.0)
        log.warning(f"{failed} registro(s) rejeitado(s) pelo Kinesis; reenviando em {wait:.1f}s")
        time.sleep(wait)

    raise RuntimeError(
        f"{len(pending)} registro(s) não aceitos pelo Kinesis após {MAX_RETRIES} reenvios"
    )


def send_records(start_row, context):
    """Envia os registros a partir de start_row (linhas de dados, sem o header).

    Retorna None quando o arquivo terminou, ou o número da próxima linha a
    enviar quando o tempo da execução está acabando — nesse caso o handler
    dispara uma continuação.
    """
    with open(TMP_PATH, encoding="utf-8") as student_file:

        reader = csv.DictReader(student_file)
        batch = []
        current_row = 0
        sent = 0

        if start_row:
            log.info(f"Retomando envio a partir da linha {start_row + 1}...")

        for row in reader:

            current_row += 1
            if current_row <= start_row:
                continue

            batch.append({
                "Data": json.dumps(row, ensure_ascii=False),
                "PartitionKey": row.get("id_aluno", "default")
            })

            if len(batch) >= BATCH_SIZE:

                put_with_retry(batch)
                sent += len(batch)
                batch = []

                if sent % LOG_EVERY == 0:
                    log.info(f"Enviados nesta execução: {sent} (até a linha {current_row})")

                if context.get_remaining_time_in_millis() < SAFETY_MARGIN_MS:
                    log.info(f"Tempo da execução quase esgotado após {sent} registros.")
                    return current_row

                time.sleep(DELAY)

        if batch:
            put_with_retry(batch)
            sent += len(batch)

    log.info(f"Concluído! Enviados nesta execução: {sent} | última linha: {current_row}")
    return None


def lambda_handler(event, context):
    start_row = int((event or {}).get("start_row", 0))

    download_csv()
    next_row = send_records(start_row, context)

    if next_row is not None:
        log.info(f"Disparando continuação a partir da linha {next_row + 1}...")
        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType="Event",
            Payload=json.dumps({"start_row": next_row}),
        )
        return {"status": "continuacao", "next_row": next_row}

    return {"status": "ok"}
