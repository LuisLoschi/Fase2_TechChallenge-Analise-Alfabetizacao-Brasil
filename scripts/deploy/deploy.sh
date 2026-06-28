#!/usr/bin/env bash
#
# deploy.sh — Provisionamento e execução da pipeline (Bronze + Silver) na AWS.
#
# Automatiza, de forma idempotente, o provisionamento e a execução da pipeline.
# Cada subcomando pode ser executado isoladamente, o que é importante no AWS
# Academy, onde as credenciais expiram a cada sessão. Veja o README desta pasta.
#
# Uso:
#   ./deploy.sh <subcomando>
#
# Subcomandos:
#   prereqs          Verifica CLI, credenciais e variáveis.
#   upload           Cria o bucket e envia dados, bases de apoio e scripts.
#   bronze           Cria/atualiza e executa o Glue Job batch da Bronze.
#   crawler-bronze   Cria/atualiza e executa o Crawler da Bronze.
#   silver           Cria/atualiza e executa o Glue Job da Silver.
#   crawler-silver   Cria/atualiza e executa o Crawler da Silver.
#   validate         Roda consultas de validação no Athena.
#   all              Executa: upload -> bronze -> crawler-bronze -> silver -> crawler-silver -> validate.
#   streaming        (opcional) Provisiona Kinesis + Glue streaming + Lambda e inicia a produção do aluno.
#   streaming-status (opcional) Mostra o progresso da ingestão do aluno na Bronze.
#   streaming-stop   (opcional) Para o Glue streaming job do aluno.
#   cleanup          Remove todos os recursos criados (evita custos).
#
# Variáveis de ambiente (opcionais — têm valores padrão):
#   REGION       (padrão: us-east-1)
#   BUCKET       (padrão: fiap-tech-challenge-2-<account-id>-<region>)
#   ROLE_ARN     (padrão: arn:aws:iam::<account-id>:role/LabRole)
#   WORKERS      (padrão: 2)   nº de workers do Glue
#   WORKER_TYPE  (padrão: G.1X)
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuração e constantes
# ---------------------------------------------------------------------------
REGION="${REGION:-us-east-1}"
# Propaga a região para todos os comandos da AWS CLI (evita o erro NoRegion
# quando o ~/.aws/config não define uma região padrão).
export AWS_DEFAULT_REGION="$REGION"
WORKERS="${WORKERS:-2}"
WORKER_TYPE="${WORKER_TYPE:-G.1X}"

STREAM_NAME="stream-alfabetizacao-aluno"
ALUNO_CSV="arquivos/br_inep_avaliacao_alfabetizacao_aluno.csv"

JOB_BRONZE="etl-bronze-alfabetizacao"
JOB_SILVER="etl-silver-alfabetizacao"
JOB_STREAM="glue-streaming-alfabetizacao-aluno"
CRAWLER_BRONZE="crawler-bronze-alfabetizacao"
CRAWLER_SILVER="crawler-silver-alfabetizacao"
DB_BRONZE="db_alfabetizacao_bronze"
DB_SILVER="db_alfabetizacao_silver"
LAMBDA_NAME="producer-student-data"

# Caminho da raiz do projeto (duas pastas acima deste script).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

log()  { echo "[$(date +%H:%M:%S)] $*"; }
fail() { echo "❌ $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Inicialização: descobre conta, define BUCKET e ROLE_ARN
# ---------------------------------------------------------------------------
init_vars() {
  command -v aws >/dev/null 2>&1 || fail "AWS CLI não encontrada. Instale a AWS CLI v2 (veja o README desta pasta)."
  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" \
    || fail "Credenciais inválidas/expiradas. Reconfigure ~/.aws/credentials (AWS Academy)."
  BUCKET="${BUCKET:-fiap-tech-challenge-2-${ACCOUNT_ID}-${REGION}}"
  ROLE_ARN="${ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/LabRole}"
}

# ---------------------------------------------------------------------------
# Funções auxiliares (espera de jobs/crawlers e Athena)
# ---------------------------------------------------------------------------
wait_job() {
  local job="$1" runid="$2" st
  while true; do
    st="$(aws glue get-job-run --job-name "$job" --run-id "$runid" --query "JobRun.JobRunState" --output text)"
    log "  [$job] $st"
    case "$st" in
      SUCCEEDED) log "  ✅ $job concluído"; return 0 ;;
      FAILED|ERROR|TIMEOUT|STOPPED) fail "$job terminou com estado $st (veja: aws logs tail /aws-glue/jobs/output --follow)" ;;
    esac
    sleep 20
  done
}

wait_crawler() {
  local name="$1" st
  while true; do
    st="$(aws glue get-crawler --name "$name" --query "Crawler.State" --output text)"
    log "  [$name] $st"
    [ "$st" = "READY" ] && { log "  ✅ $name concluído"; return 0; }
    sleep 15
  done
}

athena() {
  local q="$1" qid st
  qid="$(aws athena start-query-execution \
    --query-string "$q" \
    --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
    --query QueryExecutionId --output text)"
  while true; do
    st="$(aws athena get-query-execution --query-execution-id "$qid" --query "QueryExecution.Status.State" --output text)"
    case "$st" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$qid" \
          --query "QueryExecution.Status.StateChangeReason" --output text
        return 1 ;;
    esac
    sleep 2
  done
  aws athena get-query-results --query-execution-id "$qid" \
    --query "ResultSet.Rows[].Data[].VarCharValue" --output table
}

make_zip() {
  # Empacota um único arquivo em .zip (arquivo na raiz do pacote), usando a
  # primeira ferramenta disponível: zip, python ou powershell.
  # Opera com NOMES RELATIVOS dentro do diretório de origem para evitar problemas
  # de conversão de caminho POSIX -> Windows quando o terminal é o Git Bash.
  local src="$1" out="$2"
  local dir base outbase
  dir="$(dirname "$src")"; base="$(basename "$src")"; outbase="$(basename "$out")"
  (
    cd "$dir" || exit 1
    rm -f "$outbase"
    if command -v zip >/dev/null 2>&1; then
      zip -q -j "$outbase" "$base"
    elif command -v python >/dev/null 2>&1; then
      python -m zipfile -c "$outbase" "$base"
    elif command -v powershell >/dev/null 2>&1; then
      powershell -NoProfile -Command "Compress-Archive -Force -Path '$base' -DestinationPath '$outbase'"
    else
      exit 2
    fi
  )
  case "$?" in
    0) [ -f "$out" ] || fail "Empacotamento não gerou o arquivo $out." ;;
    2) fail "Não encontrei 'zip', 'python' nem 'powershell' para empacotar a Lambda." ;;
    *) fail "Falha ao empacotar a Lambda." ;;
  esac
}

# ---------------------------------------------------------------------------
# Funções idempotentes (criam ou atualizam recursos)
# ---------------------------------------------------------------------------
ensure_bucket() {
  if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    log "Bucket já existe: $BUCKET"
  else
    log "Criando bucket: $BUCKET"
    aws s3 mb "s3://$BUCKET" --region "$REGION"
  fi
}

ensure_glue_job() {
  local name="$1" cmd_json="$2" args_json="$3"
  if aws glue get-job --job-name "$name" >/dev/null 2>&1; then
    log "Atualizando Glue Job: $name"
    aws glue update-job --job-name "$name" --job-update \
      "{\"Role\":\"$ROLE_ARN\",\"GlueVersion\":\"4.0\",\"NumberOfWorkers\":$WORKERS,\"WorkerType\":\"$WORKER_TYPE\",\"Command\":$cmd_json,\"DefaultArguments\":$args_json}" >/dev/null
  else
    log "Criando Glue Job: $name"
    aws glue create-job --name "$name" --role "$ROLE_ARN" --glue-version "4.0" \
      --number-of-workers "$WORKERS" --worker-type "$WORKER_TYPE" \
      --command "$cmd_json" --default-arguments "$args_json" >/dev/null
  fi
}

run_glue_job() {
  local name="$1" rid
  rid="$(aws glue start-job-run --job-name "$name" --query JobRunId --output text)"
  log "Executando $name (run-id: $rid)"
  wait_job "$name" "$rid"
}

ensure_database() {
  local name="$1"
  aws glue get-database --name "$name" >/dev/null 2>&1 \
    || aws glue create-database --database-input "{\"Name\":\"$name\"}"
}

ensure_crawler() {
  local name="$1" db="$2" prefix="$3" path="$4"
  local targets="{\"S3Targets\":[{\"Path\":\"$path\"}]}"
  if aws glue get-crawler --name "$name" >/dev/null 2>&1; then
    log "Atualizando Crawler: $name"
    aws glue update-crawler --name "$name" --role "$ROLE_ARN" \
      --database-name "$db" --table-prefix "$prefix" --targets "$targets" >/dev/null
  else
    log "Criando Crawler: $name"
    aws glue create-crawler --name "$name" --role "$ROLE_ARN" \
      --database-name "$db" --table-prefix "$prefix" --targets "$targets" >/dev/null
  fi
}

run_crawler() {
  local name="$1"
  # Evita erro caso já esteja rodando.
  aws glue start-crawler --name "$name" 2>/dev/null || log "  ($name já em execução)"
  wait_crawler "$name"
}

# ---------------------------------------------------------------------------
# Subcomandos
# ---------------------------------------------------------------------------
cmd_prereqs() {
  init_vars
  log "AWS CLI: $(aws --version 2>&1)"
  log "Conta:   $ACCOUNT_ID"
  log "Região:  $REGION"
  log "Bucket:  $BUCKET"
  log "Role:    $ROLE_ARN"
  [ -f "$ROOT_DIR/src/silver/etl-silver.py" ] || fail "Script da Silver não encontrado em $ROOT_DIR/src/silver/."
  [ -d "$ROOT_DIR/data/fonte-apoio" ]        || fail "Pasta data/fonte-apoio não encontrada."
  log "✅ Pré-requisitos OK."
}

cmd_upload() {
  init_vars
  ensure_bucket
  log "Enviando CSVs do INEP..."
  aws s3 cp "$ROOT_DIR/data/fonte-dados/" "s3://$BUCKET/arquivos/" --recursive
  log "Enviando bases de apoio (necessárias para o enriquecimento da Silver)..."
  aws s3 cp "$ROOT_DIR/data/fonte-apoio/" "s3://$BUCKET/arquivos/" --recursive
  log "Enviando scripts..."
  aws s3 cp "$ROOT_DIR/src/bronze/" "s3://$BUCKET/scripts/" --recursive --exclude "*" --include "*.py"
  aws s3 cp "$ROOT_DIR/src/silver/etl-silver.py" "s3://$BUCKET/scripts/etl-silver.py"
  log "✅ Upload concluído."
}

cmd_bronze() {
  init_vars
  log "Sincronizando script da Bronze no S3..."
  aws s3 cp "$ROOT_DIR/src/bronze/etl-bronze.py" "s3://$BUCKET/scripts/etl-bronze.py" >/dev/null
  local cmd="{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/etl-bronze.py\",\"PythonVersion\":\"3\"}"
  local args="{\"--BUCKET_NAME\":\"$BUCKET\",\"--TempDir\":\"s3://$BUCKET/tmp/\"}"
  ensure_glue_job "$JOB_BRONZE" "$cmd" "$args"
  run_glue_job "$JOB_BRONZE"
}

cmd_crawler_bronze() {
  init_vars
  ensure_database "$DB_BRONZE"
  ensure_crawler "$CRAWLER_BRONZE" "$DB_BRONZE" "bronze_" "s3://$BUCKET/bronze/"
  run_crawler "$CRAWLER_BRONZE"
  aws glue get-tables --database-name "$DB_BRONZE" --query "TableList[].Name" --output table
}

cmd_silver() {
  init_vars
  log "Sincronizando script da Silver no S3..."
  aws s3 cp "$ROOT_DIR/src/silver/etl-silver.py" "s3://$BUCKET/scripts/etl-silver.py" >/dev/null
  local cmd="{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/etl-silver.py\",\"PythonVersion\":\"3\"}"
  local args="{\"--BUCKET_NAME\":\"$BUCKET\",\"--TempDir\":\"s3://$BUCKET/tmp/\"}"
  ensure_glue_job "$JOB_SILVER" "$cmd" "$args"
  run_glue_job "$JOB_SILVER"
}

cmd_crawler_silver() {
  init_vars
  ensure_database "$DB_SILVER"
  ensure_crawler "$CRAWLER_SILVER" "$DB_SILVER" "silver_" "s3://$BUCKET/silver/"
  run_crawler "$CRAWLER_SILVER"
  aws glue get-tables --database-name "$DB_SILVER" --query "TableList[].Name" --output table
}

cmd_validate() {
  init_vars
  log "Tabelas na Silver:"
  athena "SHOW TABLES IN $DB_SILVER" || true
  log "Contagem (UF):"
  athena "SELECT count(*) FROM ${DB_SILVER}.silver_avaliacao_alfabetizacao_uf" || true
  log "Decodificação de domínio (rede/serie):"
  athena "SELECT DISTINCT rede, rede_nome, serie, serie_nome FROM ${DB_SILVER}.silver_avaliacao_alfabetizacao_uf ORDER BY rede" || true
}

cmd_all() {
  cmd_upload
  cmd_bronze
  cmd_crawler_bronze
  cmd_silver
  cmd_crawler_silver
  cmd_validate
  log "✅ Pipeline batch concluída (Bronze + Silver)."
}

# --- Streaming do aluno (opcional / interativo) ----------------------------
cmd_streaming() {
  init_vars
  # 1) Stream Kinesis
  if aws kinesis describe-stream-summary --stream-name "$STREAM_NAME" >/dev/null 2>&1; then
    log "Stream já existe: $STREAM_NAME"
  else
    log "Criando stream Kinesis: $STREAM_NAME"
    aws kinesis create-stream --stream-name "$STREAM_NAME" --stream-mode-details StreamMode=ON_DEMAND
    log "Aguardando stream ficar ACTIVE..."
    aws kinesis wait stream-exists --stream-name "$STREAM_NAME"
  fi

  # 2) Glue streaming job (lê do Kinesis e grava na Bronze)
  local cmd="{\"Name\":\"gluestreaming\",\"ScriptLocation\":\"s3://$BUCKET/scripts/glue-streaming-job.py\",\"PythonVersion\":\"3\"}"
  local args="{\"--BUCKET_NAME\":\"$BUCKET\",\"--REGION\":\"$REGION\",\"--TempDir\":\"s3://$BUCKET/tmp/\"}"
  ensure_glue_job "$JOB_STREAM" "$cmd" "$args"
  # Inicia o streaming apenas se ainda não houver um run ativo (evita duplicar).
  local active
  active="$(aws glue get-job-runs --job-name "$JOB_STREAM" \
    --query "JobRuns[?JobRunState=='RUNNING'].Id | [0]" --output text 2>/dev/null || true)"
  if [ -n "$active" ] && [ "$active" != "None" ]; then
    log "Glue streaming já está em execução (run: $active)"
  else
    log "Iniciando Glue streaming (ficará aguardando mensagens)..."
    aws glue start-job-run --job-name "$JOB_STREAM" --query JobRunId --output text
  fi

  # 3) Lambda producer (renomeia para evitar hífen no módulo Python)
  local tmp; tmp="$(mktemp -d)"
  cp "$ROOT_DIR/src/bronze/producer-student-data.py" "$tmp/producer_student_data.py"
  make_zip "$tmp/producer_student_data.py" "$tmp/producer.zip"
  local env="{\"Variables\":{\"BUCKET_NAME\":\"$BUCKET\",\"CSV_PATH\":\"$ALUNO_CSV\"}}"
  # Executa de dentro do diretório do zip e usa caminho RELATIVO no fileb://,
  # pois a AWS CLI (Windows) não abre caminhos POSIX absolutos do Git Bash.
  (
    cd "$tmp" || exit 1
    if aws lambda get-function --function-name "$LAMBDA_NAME" >/dev/null 2>&1; then
      log "Atualizando Lambda: $LAMBDA_NAME"
      aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://producer.zip >/dev/null
      aws lambda update-function-configuration --function-name "$LAMBDA_NAME" --environment "$env" >/dev/null
    else
      log "Criando Lambda: $LAMBDA_NAME"
      aws lambda create-function --function-name "$LAMBDA_NAME" --runtime python3.12 \
        --role "$ROLE_ARN" --handler producer_student_data.lambda_handler \
        --timeout 900 --memory-size 512 --ephemeral-storage '{"Size":1024}' \
        --zip-file fileb://producer.zip --environment "$env" >/dev/null
    fi
  ) || fail "Falha ao criar/atualizar a Lambda."
  rm -rf "$tmp"

  # 4) Dispara a produção (assíncrona)
  log "Invocando a Lambda producer (envio assíncrono ao Kinesis)..."
  aws lambda invoke --function-name "$LAMBDA_NAME" --invocation-type Event \
    --cli-binary-format raw-in-base64-out /dev/null
  log "✅ Streaming iniciado."
  log "   Acompanhe a ingestão com:  ./deploy.sh streaming-status"
  log "   Quando os dados pararem de crescer, encerre com:  ./deploy.sh streaming-stop"
}

cmd_streaming_status() {
  init_vars
  local prefix="s3://$BUCKET/bronze/avaliacao_alfabetizacao_aluno/"
  log "Progresso da ingestão do aluno na Bronze:"
  log "  $prefix"
  if aws s3 ls "$prefix" >/dev/null 2>&1; then
    aws s3 ls "$prefix" --recursive --summarize | tail -5
  else
    log "  (ainda sem dados — aguarde alguns minutos após iniciar o streaming)"
  fi
}

cmd_streaming_stop() {
  init_vars
  local srun
  srun="$(aws glue get-job-runs --job-name "$JOB_STREAM" \
    --query "JobRuns[?JobRunState=='RUNNING'].Id | [0]" --output text 2>/dev/null || true)"
  if [ -n "$srun" ] && [ "$srun" != "None" ]; then
    log "Parando Glue streaming (run: $srun)"
    aws glue batch-stop-job-run --job-name "$JOB_STREAM" --job-run-ids "$srun" >/dev/null
    log "✅ Streaming encerrado."
  else
    log "Nenhum run de streaming em execução."
  fi
}

cmd_cleanup() {
  init_vars
  log "Removendo recursos (erros são ignorados)..."
  aws glue delete-job --job-name "$JOB_BRONZE"   2>/dev/null || true
  aws glue delete-job --job-name "$JOB_SILVER"   2>/dev/null || true
  aws glue delete-job --job-name "$JOB_STREAM"   2>/dev/null || true
  aws glue delete-crawler --name "$CRAWLER_BRONZE" 2>/dev/null || true
  aws glue delete-crawler --name "$CRAWLER_SILVER" 2>/dev/null || true
  aws glue delete-database --name "$DB_BRONZE"   2>/dev/null || true
  aws glue delete-database --name "$DB_SILVER"   2>/dev/null || true
  aws lambda delete-function --function-name "$LAMBDA_NAME" 2>/dev/null || true
  aws kinesis delete-stream --stream-name "$STREAM_NAME" 2>/dev/null || true
  aws s3 rm "s3://$BUCKET" --recursive 2>/dev/null || true
  aws s3 rb "s3://$BUCKET" 2>/dev/null || true
  log "✅ Limpeza concluída."
}

usage() {
  sed -n '3,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "${1:-}" in
  prereqs)         cmd_prereqs ;;
  upload)          cmd_upload ;;
  bronze)          cmd_bronze ;;
  crawler-bronze)  cmd_crawler_bronze ;;
  silver)          cmd_silver ;;
  crawler-silver)  cmd_crawler_silver ;;
  validate)        cmd_validate ;;
  all)             cmd_all ;;
  streaming)        cmd_streaming ;;
  streaming-status) cmd_streaming_status ;;
  streaming-stop)   cmd_streaming_stop ;;
  cleanup)         cmd_cleanup ;;
  ""|-h|--help|help) usage ;;
  *) echo "Subcomando desconhecido: $1"; echo; usage; exit 1 ;;
esac
