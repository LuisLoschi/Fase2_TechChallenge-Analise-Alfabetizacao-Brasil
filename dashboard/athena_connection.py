import logging
import os

import awswrangler as wr
import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

ATHENA_REGION = os.getenv("ATHENA_REGION", "us-east-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "db_alfabetizacao_gold")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_S3_OUTPUT = os.getenv("ATHENA_S3_OUTPUT", "") or None

GOLD_TABLE = "gold_alfabetizacao_analise_output"


class AthenaQueryError(RuntimeError):
    """Falha na execução de uma consulta no Athena."""


def _boto3_session() -> boto3.Session:
    return boto3.Session(region_name=ATHENA_REGION)


def run_query(sql: str) -> pd.DataFrame:
    """Executa uma consulta SQL no Athena e retorna o resultado em DataFrame.

    Usa `ctas_approach=False` (leitura direta do CSV de resultado): adequado
    aos volumes agregados do dashboard e não cria tabelas temporárias no Glue.

    Args:
        sql: Consulta SQL (dialeto Presto/Trino do Athena).

    Returns:
        DataFrame com o resultado da consulta.

    Raises:
        AthenaQueryError: se a consulta falhar (sintaxe, permissão, timeout).
    """
    log.info("Executando consulta no Athena (database=%s, workgroup=%s)",
             ATHENA_DATABASE, ATHENA_WORKGROUP)
    log.debug("SQL: %s", sql)
    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database=ATHENA_DATABASE,
            workgroup=ATHENA_WORKGROUP,
            s3_output=ATHENA_S3_OUTPUT,
            ctas_approach=False,
            boto3_session=_boto3_session(),
        )
    except Exception as exc:
        log.error("Consulta falhou: %s", exc)
        raise AthenaQueryError(
            f"Falha ao consultar o Athena ({type(exc).__name__}): {exc}. "
            "Verifique as credenciais AWS, a região e se a tabela "
            f"{ATHENA_DATABASE}.{GOLD_TABLE} está catalogada."
        ) from exc

    log.info("Consulta concluída: %d linhas x %d colunas", *df.shape)
    return df


def sql_literal(value: str) -> str:
    """Escapa um valor para uso como literal string em SQL (aspas simples)."""
    return "'" + value.replace("'", "''") + "'"
