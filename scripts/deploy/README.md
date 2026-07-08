# 🛠️ `deploy.sh` — Automação da Pipeline (Bronze + Silver + Gold)

Script de linha de comando que **provisiona e executa** toda a pipeline da
arquitetura medalhão (camadas Bronze, Silver e Gold) na AWS, de forma
**idempotente** e por **subcomandos**.

Ele cobre todo o caminho **batch** (Bronze → Silver → Gold → crawlers → validação) e,
separadamente, o fluxo de **streaming** do aluno. A configuração detalhada de
cada serviço (e os equivalentes no console web) está no
[`README.md`](../../README.md) principal do projeto.

---

## 📑 Sumário

- [Quando usar este script](#quando-usar)
- [Pré-requisitos](#pré-requisitos)
- [Como funciona (visão geral)](#como-funciona)
- [Subcomandos](#subcomandos)
- [Variáveis de configuração](#variáveis-de-configuração)
- [Exemplos de uso](#exemplos-de-uso)
- [O fluxo de streaming (aluno)](#fluxo-de-streaming)
- [Idempotência: por que posso rodar de novo](#idempotência)
- [Solução de problemas](#solução-de-problemas)
- [O que o script NÃO faz](#o-que-o-script-não-faz)

---

<a id="quando-usar"></a>
## Quando usar este script

| Cenário | Use |
|---|---|
| Quero subir tudo de uma vez | **este script** (`./deploy.sh all`) |
| Preciso reexecutar só uma etapa (ex.: a Silver) | **este script** (`./deploy.sh silver`) |
| Quero remover tudo ao final | **este script** (`./deploy.sh cleanup`) |

---

<a id="pré-requisitos"></a>
## Pré-requisitos

1. **AWS CLI v2** instalada (`aws --version`).
2. **Git Bash** (no Windows) — o script é Bash e usa JSON entre aspas, o que não
   funciona bem no PowerShell.
3. **Credenciais da AWS configuradas** em `~/.aws/credentials`. No AWS Academy,
   copie de *AWS Details → AWS CLI* (as credenciais expiram a cada sessão).
4. Para o subcomando `streaming`, é necessário **`zip`, `python` ou `powershell`**
   disponível no terminal (o script usa o primeiro que encontrar para empacotar a
   Lambda). No Windows, o `python` ou o `powershell` já cobrem esse caso.
5. Os **CSVs** presentes em `data/` (o arquivo de aluno tem >200 MB e é baixado à
   parte — veja o `README.md` principal).

Valide tudo de uma vez:

```bash
./deploy.sh prereqs
```

Saída esperada: versão da CLI, ID da conta, região, nome do bucket e role.

---

<a id="como-funciona"></a>
## Como funciona (visão geral)

O script é dividido em três blocos:

1. **Inicialização** (`init_vars`): descobre o ID da conta via `aws sts`, monta o
   nome do bucket e o ARN da role. Falha cedo, com mensagem clara, se a CLI não
   existir ou as credenciais estiverem expiradas.
2. **Funções idempotentes** (`ensure_*`): cada recurso é criado **ou atualizado**
   se já existir (jobs, crawlers, bucket, databases, Lambda, stream).
3. **Subcomandos** (`cmd_*`): orquestram as funções acima na ordem correta e
   aguardam a conclusão de jobs e crawlers (que são assíncronos) antes de seguir.

Fluxo do subcomando `all`:

```text
upload ─▶ bronze ─▶ crawler-bronze ─▶ silver ─▶ crawler-silver ─▶ gold ─▶ crawler-gold ─▶ validate
```

---

<a id="subcomandos"></a>
## Subcomandos

| Subcomando | O que faz |
|---|---|
| `prereqs` | Verifica CLI, credenciais e arquivos do projeto. |
| `upload` | Cria o bucket e envia dados, **bases de apoio** e scripts para o S3. |
| `bronze` | Cria/atualiza e executa o Glue Job batch da Bronze. |
| `crawler-bronze` | Cria o banco `db_alfabetizacao_bronze` e roda o crawler. |
| `silver` | Cria/atualiza e executa o Glue Job da Silver. |
| `crawler-silver` | Cria o banco `db_alfabetizacao_silver` e roda o crawler. |
| `gold` | Cria/atualiza e executa o Glue Job da Gold. |
| `crawler-gold` | Cria o banco `db_alfabetizacao_gold` e roda o crawler. |
| `validate` | Roda consultas de validação no Athena (Silver e Gold). |
| `all` | Executa a sequência completa do caminho **batch**. |
| `streaming` | *(opcional)* Provisiona Kinesis + Glue streaming + Lambda e inicia a produção do aluno. |
| `streaming-status` | *(opcional)* Mostra o progresso da ingestão do aluno na Bronze. |
| `streaming-stop` | *(opcional)* Para o Glue streaming job do aluno. |
| `cleanup` | Remove todos os recursos criados (evita custos). |
| `help` | Mostra a ajuda. |

> O subcomando `all` **não** inclui o `streaming`, por ele ser opcional e
> interativo (veja a seção [streaming](#fluxo-de-streaming)).

---

<a id="variáveis-de-configuração"></a>
## Variáveis de configuração

Todas têm valor padrão e podem ser sobrescritas por variável de ambiente — **sem
editar o script**:

| Variável | Padrão | Descrição |
|---|---|---|
| `REGION` | `us-east-1` | Região da AWS. |
| `BUCKET` | `fiap-tech-challenge-2-<account-id>-<region>` | Nome do bucket S3. |
| `ROLE_ARN` | `arn:aws:iam::<account-id>:role/LabRole` | Role usada pelos serviços. |
| `WORKERS` | `2` | Nº de workers do Glue. |
| `WORKER_TYPE` | `G.1X` | Tipo de worker do Glue. |

Exemplo (bucket e mais workers para acelerar a Silver):

```bash
BUCKET="meu-bucket-personalizado" WORKERS=5 ./deploy.sh silver
```

---

<a id="exemplos-de-uso"></a>
## Exemplos de uso

```bash
# Entrar na pasta do script
cd scripts/deploy

# 0. Conferir pré-requisitos
./deploy.sh prereqs

# 1. Subir tudo (caminho batch completo)
./deploy.sh all

# Ou executar etapa por etapa
./deploy.sh upload
./deploy.sh bronze
./deploy.sh crawler-bronze
./deploy.sh silver
./deploy.sh crawler-silver
./deploy.sh gold
./deploy.sh crawler-gold
./deploy.sh validate

# 2. Ao terminar, remover os recursos
./deploy.sh cleanup
```

> Os subcomandos de Glue (`bronze`, `silver`) e de crawler **bloqueiam** até a
> tarefa terminar, exibindo o estado a cada poucos segundos. Não é preciso
> esperar manualmente.

---

<a id="fluxo-de-streaming"></a>
## O fluxo de streaming (aluno)

A tabela de alunos é ingerida por **streaming** (Kinesis), e não em batch. Por
envolver passos assíncronos e interativos (produzir dados e depois encerrar o
job), ela fica **fora** do `all`:

```bash
# 1. Provisiona o stream, o Glue streaming e a Lambda, e inicia a produção
./deploy.sh streaming

# 2. Acompanhe os dados chegando na Bronze (aguarde alguns minutos)
./deploy.sh streaming-status

# 3. Quando os dados pararem de crescer, encerre o job de streaming
./deploy.sh streaming-stop

# 4. Reprocesse a Silver e a Gold para incluir a entidade de aluno
./deploy.sh silver
./deploy.sh crawler-silver
./deploy.sh gold
./deploy.sh crawler-gold
```

> O script já trata os dois detalhes que costumam quebrar este fluxo via CLI:
> renomeia o arquivo da Lambda (`producer-student-data.py` →
> `producer_student_data.py`, pois o Python não importa módulos com hífen) e
> **não** define a variável reservada `AWS_REGION`.

---

<a id="idempotência"></a>
## Idempotência: por que posso rodar de novo

Cada recurso é **criado ou atualizado** — nunca duplicado. Se um Glue Job já
existe, o script faz `update-job` em vez de falhar no `create-job`. O mesmo vale
para crawlers, bancos, bucket, Lambda e stream.

Na prática, isso significa:

- Rodar `./deploy.sh all` duas vezes **não gera erro**.
- Corrigiu o script da Silver? Rode `./deploy.sh silver` de novo — ele atualiza o
  job e reexecuta.
- A sessão do AWS Academy expirou no meio? Renove as credenciais e rode de novo o
  subcomando que faltou.

---

<a id="solução-de-problemas"></a>
## Solução de problemas

| Mensagem / sintoma | Causa | Solução |
|---|---|---|
| `Credenciais inválidas/expiradas` | Sessão do AWS Academy expirou | Atualize `~/.aws/credentials` e rode de novo |
| `AWS CLI não encontrada` | CLI não instalada/no PATH | Instale a AWS CLI v2 |
| Job Silver falha lendo `/arquivos/br_bd_*` | Bases de apoio não enviadas | Rode `./deploy.sh upload` |
| JSON quebra / erros de aspas | Está usando PowerShell | Use **Git Bash** |
| `<job> terminou com estado FAILED` | Erro no script Glue | `aws logs tail /aws-glue/jobs/output --follow` |
| `NoRegion` em comando `aws` manual | Região não definida no terminal | `export AWS_DEFAULT_REGION=us-east-1` |
| `ano = 2023` falha com `TYPE_MISMATCH` | `ano` é coluna de partição (texto) | Filtre com aspas: `ano = '2023'` |

---

<a id="o-que-o-script-não-faz"></a>
## O que o script NÃO faz

Por decisão de escopo (manter a solução simples e adequada ao AWS Academy):

- **Não cria IAM Roles** — usa a `LabRole` existente.
- **Não usa IaC** (Terraform/CloudFormation) — é um orquestrador de CLI direto.
- **Não faz processamento incremental** — os jobs reprocessam o conjunto completo.
- **Não inclui o streaming no `all`** — esse fluxo é executado e encerrado à parte.

Esses pontos são candidatos naturais a evolução futura, caso a pipeline saia do
contexto acadêmico para um ambiente produtivo.