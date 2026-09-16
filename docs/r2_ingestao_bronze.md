# Marco R2 — Ingestão/Bronze Transferegov

Este documento descreve a arquitetura, o fluxo de dados, as regras técnicas e as instruções operacionais da camada **Bronze** do Lakehouse de Transferências Discricionárias e Legais.

---

## 1. Visão Geral da Arquitetura

O pipeline implementa a ingestão dos dados públicos oficiais do Transferegov (SICONV), garantindo proveniência, imutabilidade da camada RAW, fidelidade de representação tabular no Delta Lake e auditoria persistente.

```
[ Fonte Oficial Transferegov (HTTPS) ]
                    │
                    ▼
     [ 1. Controle por data_carga ] ── (NO_CHANGE? Se igual e íntegro, encerra)
                    │
                    ▼ (Novo snapshot ou force=True)
[ 2. Download Seguro em Chunks (64KB) + SHA-256 ] ──► [ .part -> Promoção Atômica ]
                    │
                    ▼
    [ 3. Preservação RAW byte a byte no MinIO ]
    (s3://bronze/raw/transferegov/<dataset>/sha256=<hash>/<arquivo>.zip)
                    │
                    ▼
    [ 4. Extração Controlada do Membro CSV ] (Anti-Zip Slip)
                    │
                    ▼
    [ 5. Validação Estrutural Streaming CSV ]
    (Decodificação estrita utf-8-sig, contagem lógica de linhas, checagem de largura)
                    │
                    ▼
    [ 6. Processamento e Gravação Delta no Apache Spark ]
    (StringType para todos os campos oficiais, anexação de 4 colunas técnicas, overwrite)
    (s3a://bronze/warehouse/<dataset>)
                    │
                    ▼
    [ 7. Catálogo no Spark Thrift Server / Hive Metastore ]
    (CREATE SCHEMA / TABLE IF NOT EXISTS ... USING DELTA, REFRESH TABLE)
                    │
                    ▼
    [ 8. Reconciliação Estrutural de Contagens ]
    (CSV lógico == Delta gravado == Thrift SELECT COUNT(*))
                    │
                    ▼
    [ 9. Auditoria Persistente Delta ]
    (bronze.ingestion_runs e bronze.ingestion_manifest)
                    │
                    ▼
    [ 10. Política de Retenção RAW (Atual + Anterior = 2 Versões) ]
                    │
                    ▼
    [ 11. Limpeza de Diretórios Temporários ]
```

---

## 2. Datasets Ingeridos

| Identificador do Dataset | Arquivo ZIP Oficial | Membro CSV Extraído | Tabela Delta Bronze | Localização Warehouse |
|---|---|---|---|---|
| `siconv_programa` | `siconv_programa.zip` | `siconv_programa.csv` | `bronze.siconv_programa` | `s3a://bronze/warehouse/siconv_programa` |
| `siconv_programa_proposta` | `siconv_programa_proposta.zip` | `siconv_programa_proposta.csv` | `bronze.siconv_programa_proposta` | `s3a://bronze/warehouse/siconv_programa_proposta` |
| `siconv_proposta` | `siconv_proposta.zip` | `siconv_proposta.csv` | `bronze.siconv_proposta` | `s3a://bronze/warehouse/siconv_proposta` |
| `siconv_convenio` | `siconv_convenio.zip` | `siconv_convenio.csv` | `bronze.siconv_convenio` | `s3a://bronze/warehouse/siconv_convenio` |
| `data_carga_siconv` *(Controle)* | `data_carga_siconv.zip` | `data_carga_siconv.csv` | *(Tabelas de Auditoria)* | `s3://bronze/raw/transferegov/data_carga_siconv/...` |

---

## 3. Diretrizes e Regras Técnicas Implementadas

### 3.1. Parsing Fiel
- **Tipo de Dados:** Todas as colunas oficiais são forçadas para `StringType`. Não há inferência de datas, números ou valores monetários.
- **Zeros à Esquerda e Espaços:** Preservados integralmente conforme publicados pela fonte oficial.
- **Codificação e Delimitador:** `utf-8-sig` (removendo BOM de forma limpa), delimitador `;`, aspas duplas como delimitador de texto e suporte a quebras de linha entre aspas (`multiLine=True`).
- **Validação de Cabeçalhos:** Rejeição de colunas sem nome, colunas duplicadas ou nomes que colidam com metadados técnicos.
- **Validação de Largura:** Cada registro do CSV é validado para conter rigorosamente a mesma quantidade de campos do cabeçalho.
- **Reconciliação:** A contagem lógica de registros obtida no streaming independente do CSV é reconciliada contra a contagem da tabela Delta gravada.

### 3.2. Metadados Técnicos Exclusivos
Apenas quatro colunas técnicas são adicionadas à representação tabular:
1. `__ingested_at_utc`: Carimbo temporal ISO UTC do momento da ingestão.
2. `__ingestion_run_id`: Identificador único da execução.
3. `__source_file`: Nome do arquivo ZIP original publicado.
4. `__source_sha256`: Hash SHA-256 do arquivo original correspondente aos bytes armazenados no RAW.

### 3.3. Preservação RAW e MinIO
- Armazenamento em: `s3://bronze/raw/transferegov/<dataset>/sha256=<hash>/<arquivo>.zip`.
- Imutabilidade comprovada: rejeição de envio caso já exista chave com tamanho de bytes diferente.

### 3.4. Auditoria Persistente
Duas tabelas Delta governadas no schema `bronze` e consultáveis via Spark Thrift Server:
- `bronze.ingestion_runs`: Histórico por execução (`run_id`, início/fim UTC, status, `force`, data_carga inicial/final, totais e status da retenção).
- `bronze.ingestion_manifest`: Histórico detalhado por dataset (`run_id`, `dataset_id`, URLs, hashes, tamanhos, colunas, contagens reconciliadas, caminhos RAW/Delta, versão Delta e status). O arquivo de controle `data_carga_siconv` também é formalmente ativado nesta tabela após a validação global com status `SUCCESS`.

### 3.5. Consistência Temporal e Contrato de Controle
- O arquivo `data_carga_siconv.csv` obedece a contrato estrito: exatamente 1 coluna (`data_carga`), exatamente 1 registro, formato `%d/%m/%Y %H:%M:%S` calendarizável e codificação estrita.
- As etapas `control_initial` e `control_final` gravam arquivos de evidência (`control_initial.json` e `control_final.json`) e decisão (`decision.txt`) sob `/data/staging/<run_id>/`.
- A validação global compara o controle obtido antes dos datasets contra o controle obtido após os datasets sem re-downloads, reprovando execuções onde a fonte tenha mudado durante o processo.

### 3.6. Política de Retenção RAW (Atual + Anterior = 2 Versões)
- Mantém por dataset (analíticos e controle `data_carga_siconv`) a revisão ativa atual e a revisão distinta imediatamente anterior ativada com sucesso via manifesto.
- Algoritmo implementado na função pura `plan_dataset_retention()`.
- Reexecuções do mesmo conteúdo não contam como nova versão.
- Não executa em cargas parciais ou com falha.
- Exclusão restrita a chaves exatas confirmadas no MinIO, com atualização correspondente no manifesto.

---

## 4. Integração Contínua (CI)

O repositório possui workflow GitHub Actions em `.github/workflows/ci.yml` configurado para `pull_request` (visando `main`) e `push` em `main`.
- Verificação de formatação e quebras de linha (`git diff --check`).
- Compilação de sintaxe e bytecode Python (`python -m compileall spark airflow/dags`).
- Validação estrutural do arquivo YAML de configuração de fontes (`spark/transferegov_sources.yml`).
- Execução isolada de 51 testes unitários que independem de cluster Spark/MinIO (`test_control.py`, `test_csv_processor.py`, `test_download_and_s3.py`, `test_retention.py`, `test_audit_safety.py`).

### 4.1. Segunda Rodada Corretiva do PR #1
1. **Bootstrap limpo das tabelas de auditoria (Finding 1):** Ordem estrita de inicialização garantida em `bootstrap_audit_and_catalog()`: 1) SparkSession, 2) AuditManager (cria/valida integridade física Delta de `ingestion_runs` e `ingestion_manifest`), 3) CatalogManager (`ensure_schema` e `register_delta_table`), 4) Validação de leitura externa pelo Thrift Server (`query_count`). Tabelas corrompidas no storage impedem registro no catálogo e propagam `RuntimeError`.
2. **Snapshot ativo restrito a runs globalmente SUCCESS (Finding 2):** `get_active_manifest_for_dataset()` e helper puro `find_active_manifest_record()` exigem `manifest.status == 'SUCCESS' AND ingestion_runs.status == 'SUCCESS'` para o mesmo `ingestion_run_id`. Manifestos individuais de execuções com validação global `FAILED` ou `RUNNING` são estritamente rejeitados.
3. **Resiliência do NO_CHANGE e propagação de erros S3:** `S3StorageManager.object_exists()` / `raw_key_exists()` trata ausência de objeto (404 / `NoSuchKey`) retornando `False`, fazendo com que `verify_local_integrity()` rejeite `NO_CHANGE` e prossiga com reprocessamento para recuperação. Erros de infraestrutura (500 `InternalError`, 403 `AccessDenied`, falha de rede) propagam exceção imediatamente, sem mascaramento.

## 5. Orquestração e Operação

### 5.1. DAG do Airflow
- **DAG ID:** `r2_ingestao_transferegov_bronze`
- **Agendamento:** Manual (`schedule=None`, `catchup=False`, `max_active_runs=1`).
- **Parâmetro:** `force` (booleano, padrão `False`).

### 5.2. Execução via Linha de Comando (CLI)
Dentro do container `airflow`:

```bash
# Execução completa ponta a ponta
python /app/ingest_transferegov.py --action full_run

# Execução forçada (revalidação completa)
python /app/ingest_transferegov.py --action full_run --force

# Execução com dry-run da retenção
python /app/ingest_transferegov.py --action full_run --dry-run-retention

# Execução granular de etapa
python /app/ingest_transferegov.py --action preflight --run-id manual_test_01
python /app/ingest_transferegov.py --action control_initial --run-id manual_test_01
python /app/ingest_transferegov.py --action ingest_dataset --dataset siconv_programa --run-id manual_test_01
```
