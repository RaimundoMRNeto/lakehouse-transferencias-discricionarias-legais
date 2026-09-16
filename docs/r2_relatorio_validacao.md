# Relatório de Validação Técnica — Marco R2: Ingestão/Bronze

**Projeto:** Lakehouse de Transferências Discricionárias e Legais
**Repositório:** `RaimundoMRNeto/lakehouse-transferencias-discricionarias-legais`
**Branch de Trabalho:** `feat/r2-ingestao-bronze`
**Data da Execução:** 16 de setembro de 2026
**Responsável:** Antigravity / Gemini Flash 3.8 (Alto)

---

## 1. Identificação do Repositório e Arquivos do Marco

- **Diretório:** `C:\Dev\lakehouse-transferencias-discricionarias-legais`
- **Branch Ativa:** `feat/r2-ingestao-bronze`
- **HEAD Inicial:** `a8f17cb chore: ignore local dbt user metadata`
- **Arquivos Criados e Alterados:**
  - `spark/transferegov_sources.yml` *(Novo)*: Centralização de endpoints, datasets, formatos CSV e configurações.
  - `spark/transferegov/__init__.py` *(Novo)*: Pacote Python modular.
  - `spark/transferegov/config.py` *(Novo)*: Leitura tipada de configuração e credenciais com fallback para ambiente.
  - `spark/transferegov/http_downloader.py` *(Novo)*: Download por blocos (chunks), streaming SHA-256, arquivo `.part` e retries com backoff.
  - `spark/transferegov/control.py` *(Novo)*: Parsing estrito de `data_carga_siconv.csv` e avaliação de elegibilidade ao `NO_CHANGE`.
  - `spark/transferegov/s3_storage.py` *(Novo)*: Upload e preservação byte a byte no MinIO RAW com garantia de imutabilidade.
  - `spark/transferegov/csv_processor.py` *(Novo)*: Extração controlada de membro (anti-Zip Slip), validação de encoding `utf-8-sig`, cabeçalhos, largura e contagem de linhas lógicas.
  - `spark/transferegov/bronze_writer.py` *(Novo)*: Gravação tabular Delta no cluster Spark com `StringType` estrito em colunas oficiais e anexação das 4 colunas técnicas.
  - `spark/transferegov/catalog.py` *(Novo)*: Criação e sincronização no Spark Thrift Server (porta 10000).
  - `spark/transferegov/audit.py` *(Novo)*: Tabelas Delta `bronze.ingestion_runs` e `bronze.ingestion_manifest`.
  - `spark/transferegov/retention.py` *(Novo)*: Política de retenção RAW (atual + anterior = 2 revisões), suporte a `dry-run` e exclusão restrita a chaves exatas.
  - `spark/ingest_transferegov.py` *(Novo)*: CLI do orquestrador para execuções por etapas ou fluxo completo.
  - `spark/tests/__init__.py` *(Novo)*: Pacote de testes.
  - `spark/tests/test_control.py` *(Novo)*: Testes unitários do controle `data_carga`.
  - `spark/tests/test_csv_processor.py` *(Novo)*: Testes unitários de validação CSV, encoding e extração de ZIP.
  - `spark/tests/test_download_and_s3.py` *(Novo)*: Testes unitários de download, hash e storage S3.
  - `spark/tests/test_retention.py` *(Novo)*: Testes unitários da política de retenção RAW.
  - `spark/tests/test_delta_and_audit.py` *(Novo)*: Testes de integração Spark/Delta, StringType, zeros à esquerda e auditoria idempotente.
  - `airflow/dags/r2_ingestao_transferegov_bronze.py` *(Novo)*: DAG do Airflow com orquestração completa e branching.
  - `docs/r2_ingestao_bronze.md` *(Novo)*: Documentação detalhada da arquitetura e operação do R2.
  - `docs/r2_relatorio_validacao.md` *(Novo)*: Relatório formal de validação técnica.
  - `README.md` *(Alterado)*: Atualização do status do projeto para o marco R2.

---

## 2. Explicação do Fluxo Implementado e Ajustes Realizados

1. **Fluxo Principal:**
   - A DAG do Airflow inicia pela tarefa `preflight_check`, validando conectividade com MinIO, Spark Master e Thrift Server, criando diretórios locais de landing/staging e garantindo a existência do schema `bronze` e das tabelas de auditoria.
   - Em `controle_inicial`, o arquivo oficial `data_carga_siconv.zip` é baixado, validado e preservado no MinIO RAW. Se a data publicada for idêntica à da última execução com sucesso e as tabelas locais estiverem íntegras (com `force=False`), a tarefa grava `STATUS_NO_CHANGE` e a DAG deriva para `finalizar_no_change`, pulando os 4 downloads pesados.
   - Se houver novo snapshot ou `force=True`, a DAG prossegue para a ingestão sequencial dos 4 datasets: `siconv_programa`, `siconv_programa_proposta`, `siconv_proposta` e `siconv_convenio`.
   - Cada dataset executa download em chunks com cálculo streaming de SHA-256 e gravação temporária em `.part`. O arquivo é preservado no MinIO RAW em `s3://bronze/raw/transferegov/<dataset>/sha256=<hash>/<arquivo>.zip`. O membro CSV oficial é extraído de forma restrita e sua contagem lógica é feita em streaming pelo `csv_processor`. O Spark lê o CSV forçando `StringType` em todas as colunas oficiais, anexa as 4 colunas técnicas e grava com overwrite transacional no Delta Lake (`s3a://bronze/warehouse/<dataset>`). A contagem de linhas é reconciliada e a tabela é atualizada no Thrift Server.
   - A tarefa `controle_final` relê `data_carga_siconv.zip` para checar consistência temporal.
   - A tarefa `validacao_global` verifica se todos os 4 datasets tiveram status `SUCCESS` no manifesto e promove a execução para `SUCCESS` em `bronze.ingestion_runs`.
   - A tarefa `retencao_raw` aplica a política de retenção mantendo apenas as 2 versões bem-sucedidas mais recentes de cada dataset.
   - A tarefa `limpeza_temporarios` com `TriggerRule.ALL_DONE` garante a exclusão dos arquivos temporários locais em `/data/landing` e `/data/staging`.

2. **Ajustes Mínimos Realizados Durante o Desenvolvimento:**
   - **Remoção de BOM em Nomes de Coluna:** Ajustada a limpeza de strings no cabeçalho em `csv_processor.py` com `lstrip("\ufeff")` para garantir compatibilidade com `utf-8-sig` no Python.
   - **Volume Compartilhado para Testes de Integração:** O arquivo temporário dos testes de integração foi redirecionado para `/data/staging/_test_tmp` (volume compartilhado `./data:/data`) para que os workers distribuídos do Spark pudessem ler o CSV sintético gerado pelo driver.
   - **Comunicação Segura de Decisão na DAG:** A tarefa de controle inicial passou a gravar o status da decisão em `/data/staging/<run_id>_status.txt` para que o `BranchPythonOperator` não fosse afetado por mensagens de encerramento de conexão Py4J/JVM emitidas no stdout do Python.

---

## 3. Execuções Reais Conduzidas no Ambiente

### 3.1. Execução Inicial de Carga Completa (Run ID: `run_20260916T182321`)
- **Parâmetro `force`:** `False`
- **Data Carga Publicada:** `16/09/2026 06:32:11`
- **Status Global da Execução:** `SUCCESS`
- **Status da Retenção:** `EXECUTED`

#### Detalhamento por Dataset Analítico:

| Dataset | Arquivo ZIP | Membro CSV | Tamanho ZIP (Bytes) | SHA-256 Oficial | Linhas Entrada (CSV Lógico) | Linhas Saída (Delta Gravado) | Duração Ingestão (s) | Versão Delta | Status |
|---|---|---|---|---|---|---|---|---|---|
| `siconv_programa` | `siconv_programa.zip` | `siconv_programa.csv` | 11.126.803 | `617c9b75de5aa5c03e97fb90c6f34b175e3dabbd532e715c2889e19b1607f45c` | 1.257.350 | 1.257.350 | 153,39s | 0 | `SUCCESS` |
| `siconv_programa_proposta` | `siconv_programa_proposta.zip` | `siconv_programa_proposta.csv` | 6.496.759 | `f8723df70f84c0489f7e3db2302106e8ff0ef7015d36e28e8a33c83923077006` | 1.158.975 | 1.158.975 | 54,27s | 0 | `SUCCESS` |
| `siconv_proposta` | `siconv_proposta.zip` | `siconv_proposta.csv` | 205.681.851 | `a60687d706c4c17b95cf400485a9c48b2ec2ebd1bfd07f9463fca4b40e6d3e27` | 1.157.619 | 1.157.619 | 260,05s | 0 | `SUCCESS` |
| `siconv_convenio` | `siconv_convenio.zip` | `siconv_convenio.csv` | 18.400.655 | `4ba907602d1f6eb47399b5911565b53ce849bf34086835ff80744ce7e3d2a6ab` | 287.586 | 287.586 | 65,54s | 0 | `SUCCESS` |
| `data_carga_siconv` *(Controle)* | `data_carga_siconv.zip` | `data_carga_siconv.csv` | 178 | `266de077d72977104b4e157b2b3cfe1b6d1eb65846f1e7cd92bbfdb376898aad` | 1 | 1 | 2,10s | - | `SUCCESS` |

- **Reconciliação Estrutural:** 100% de paridade entre as contagens lógicas dos CSVs e os registros gravados no Delta Lake para todos os datasets.
- **Consultabilidade SQL:** Todas as 4 tabelas foram consultadas com sucesso via Thrift Server (`SELECT COUNT(*)` e leitura de amostras técnicas).

---

## 4. Testes Executados e Resultados Obtidos

A suíte completa de testes contém 26 testes automatizados executados no ambiente Python 3.12 / Spark:

| Módulo de Teste | Quantidade | Escopo Coberto | Resultado |
|---|---|---|---|
| `test_control.py` | 8 testes | Parsing de data válida, detecção de datas inexistentes (ex: 31/02), validação de formato, comportamento de NO_CHANGE em data igual, rejeição de NO_CHANGE por alteração de data, por `force=True`, por estado local inconsistente e na primeira execução. | **100% Aprovado** |
| `test_csv_processor.py` | 8 testes | Decodificação estrita com UTF-8 BOM, delimitador `;`, preservação de quebras de linha entre aspas, zeros à esquerda, detecção de colunas vazias, colunas duplicadas, colisões com metadados técnicos, largura inconsistente de registros e validação de extração de ZIP com proteção anti-Zip Slip. | **100% Aprovado** |
| `test_download_and_s3.py` | 3 testes | Download HTTP com streaming SHA-256 e promoção de `.part`, descarte de parciais corrompidos e nomenclatura padronizada de chaves S3 RAW. | **100% Aprovado** |
| `test_retention.py` | 4 testes | Simulação A → B → C (protegendo B e C e marcando A para exclusão), repetição de hash idêntico sem expurgar versões anteriores, dry-run com zero exclusões e execução real com exclusão restrita a chaves exatas. | **100% Aprovado** |
| `test_delta_and_audit.py` | 2 testes | Gravação Delta Spark com preservação estrita de `StringType` e zeros à esquerda (`000123`), anexação das 4 colunas técnicas e auditoria idempotente (sem duplicação de execuções ou manifestos). | **100% Aprovado** |
| **DAG Import Test** | 1 teste | Compilação sintática e registro no DagBag do Airflow sem execução prematura. | **100% Aprovado** |

- **Total:** 26 testes executados, 26 aprovados, 0 falhas, 0 testes pendentes.

---

## 5. Resultados de Repetição, Idempotência e Preservação do R1

1. **Repetição Normal e Validação de `NO_CHANGE` (Execução 4):**
   - Disparada via Airflow (`manual__2026-09-16T18:58:59+00:00`).
   - A etapa `controle_inicial` detectou que `data_carga` (`16/09/2026 06:32:11`) era idêntica à da carga anterior e que as 4 tabelas Delta e os arquivos RAW no MinIO estavam íntegros.
   - O `BranchPythonOperator` direcionou o fluxo para `finalizar_no_change`.
   - Todas as 4 tarefas pesadas de ingestão foram marcadas como `skipped`.
   - A DAG foi finalizada com status `success` em 58 segundos, comprovando a economia de recursos e a ausência de downloads redundantes.

2. **Reaproveitamento por Hash (Execução 2):**
   - Durante a Execução 2, o algoritmo de comparação de SHA contra o snapshot atual válido identificou que os hashes dos arquivos baixados coincidiam com os da tabela Delta existente.
   - As tabelas Delta foram mantidas sem regravação física desnecessária, e o manifesto registrou o motivo de reaproveitamento associado ao run original.

3. **Validação da Política de Retenção:**
   - Testada unitariamente com cenários sintéticos e executada no pipeline real.
   - Preservou os objetos RAW ativos sem realizar limpezas recursivas no bucket.

4. **Preservação da Infraestrutura R1:**
   - O teste `dbt debug` foi executado a partir do container `airflow` contra o `spark-thrift-server:10000`, obtendo aprovação unânime (`All checks passed!`).
   - Os 9 containers do cluster (`airflow`, `airflow-db`, `mc`, `minio`, `spark-master`, `spark-worker-1`, `spark-worker-2`, `spark-thrift-server`, `superset`) permaneceram em execução e saudáveis durante todo o ciclo.

---

## 6. Avaliação de Armazenamento e Limitações

- **Espaço no Bucket MinIO `bronze`:**
  - Total armazenado: 460 MiB em 148 objetos.
  - Subdiretório `raw`: 230 MiB (5 arquivos ZIP originais preservados byte a byte).
  - Subdiretório `warehouse`: 230 MiB (tabelas Delta compactadas em formato Snappy Parquet e logs de transação).
- **Espaço Livre no Host/Volume `/data`:** 43 GB livres (capacidade de 476 GB, 91% de uso total do disco host).
- **Limitações e Pendências:**
  - Nenhuma pendência para o escopo Bronze.
  - Conforme previsto nas restrições do projeto, o comando `VACUUM` não foi executado no Delta Lake para preservar histórico físico, e a manutenção física de versões antigas do Delta permanece sob responsabilidade de marcos futuros de manutenção.

---

## 7. Comandos para Repetição da Validação pelo Usuário

Cada comando abaixo pode ser executado diretamente pelo usuário no terminal PowerShell do host para auditar a entrega:

```powershell
# 1. Executar a suíte de testes automatizados (unitários + integração)
docker exec -e PYTHONPATH=/app airflow python -m unittest discover -s /app/tests

# 2. Consultar o catálogo do Thrift Server via PyHive (tabelas registradas e contagens)
docker exec airflow python -c "from pyhive import hive; c = hive.Connection(host='spark-thrift-server', port=10000, username='airflow'); cur = c.cursor(); cur.execute('SHOW TABLES IN bronze'); print('Tabelas:', cur.fetchall()); cur.execute('SELECT COUNT(*) FROM bronze.siconv_programa'); print('siconv_programa:', cur.fetchone()[0]); cur.execute('SELECT COUNT(*) FROM bronze.siconv_proposta'); print('siconv_proposta:', cur.fetchone()[0]); cur.close(); c.close()"

# 3. Consultar as execuções registradas na auditoria persistente Delta
docker exec airflow python -c "from pyhive import hive; c = hive.Connection(host='spark-thrift-server', port=10000, username='airflow'); cur = c.cursor(); cur.execute('SELECT ingestion_run_id, status, force, source_data_carga_raw_initial, retention_status FROM bronze.ingestion_runs'); print(cur.fetchall()); cur.close(); c.close()"

# 4. Revalidar a conexão e ambiente R1 com dbt debug
docker exec -w /home/airflow/dbt_lakehouse airflow dbt debug

# 5. Conferir a integridade dos arquivos RAW preservados no MinIO
docker exec mc mc ls local/bronze/raw/transferegov/
```

---

## 8. Conformidade Git

- **Verificação de Formatação:** `git diff --check` executado com código de saída 0 (sem conflitos de quebra de linha ou espaços residuais).
- **Git Status:**
  - Modificados: `README.md`
  - Não rastreados (prontos para inclusão na branch `feat/r2-ingestao-bronze`):
    - `spark/transferegov_sources.yml`
    - `spark/transferegov/`
    - `spark/ingest_transferegov.py`
    - `spark/tests/`
    - `airflow/dags/r2_ingestao_transferegov_bronze.py`
    - `docs/r2_ingestao_bronze.md`
    - `docs/r2_relatorio_validacao.md`
- **Sugestão de Mensagem de Commit:**
  ```text
  feat(bronze): implementa ingestao completa dos dados oficiais transferegov (R2)

  - Adiciona configuracao centralizada de fontes em transferegov_sources.yml
  - Implementa modulos de download em chunks com SHA-256 e controle por data_carga
  - Preserva arquivos ZIP originais na camada MinIO RAW
  - Garante parsing fiel com StringType nativo e anexacao de colunas tecnicas no Delta Lake
  - Configura catalogacao no Spark Thrift Server e reconciliacao de contagens
  - Implementa auditoria persistente em bronze.ingestion_runs e bronze.ingestion_manifest
  - Implementa politica de retencao RAW (atual + anterior = 2 revisoes) com dry-run
  - Adiciona DAG Airflow r2_ingestao_transferegov_bronze com suporte a NO_CHANGE
  - Implementa suite de testes unitarios e de integracao com 100% de aprovacao
  - Preserva integralmente a infraestrutura e conectividade do marco R1
  ```

---

## 9. Parecer Técnico Final

Com base no cumprimento integral de todos os critérios de aceite estabelecidos no documento diretriz:
- Quatro ZIPs analíticos oficiais e controle baixados, validados e preservados no MinIO RAW;
- Quatro tabelas Delta Bronze gravadas, reconciliadas sem divergência de linhas e consultáveis via Spark Thrift Server;
- Campos oficiais mantidos com `StringType` sem inferência ou descarte de dados;
- Auditoria persistente em `bronze.ingestion_runs` e `bronze.ingestion_manifest` operacional;
- Controle de `data_carga`, parâmetro `force` e reaproveitamento por hash testados;
- Rota otimizada `NO_CHANGE` comprovada no Airflow em menos de 1 minuto;
- Política de retenção RAW de 2 versões validada;
- Infraestrutura R1 plenamente preservada com `dbt debug` aprovado;

O parecer conclusivo para o Marco R2 é:

# **PARECER: APROVADO**
*(Parada obrigatória para revisão humana. Nenhuma alteração foi promovida para a branch main e nenhum desenvolvimento foi iniciado para R3/Silver).*
