# Lakehouse de Transferências Discricionárias e Legais da União

Trabalho acadêmico desenvolvido na **Especialização em Ciência de Dados da Universidade Tecnológica Federal do Paraná (UTFPR)**, na disciplina de **Arquitetura de Dados**, ministrada pelo **Prof. Ms. Weslley G. de Moura**.

**Aluno:** Raimundo de Moura Rolim Neto

- Curso: [Especialização em Ciência de Dados — UTFPR](https://www.utfpr.edu.br/cursos/especializacao/dv/ciencia-de-dados)
- Disciplina: [Arquitetura de Dados — ementa oficial](https://coens.dv.utfpr.edu.br/pos/ciencia-dados/disciplinas.html)

---

## 1. Objetivo do trabalho

O objetivo deste projeto foi construir, em ambiente local, uma arquitetura de dados do tipo **Lakehouse** usando dados públicos do **Transferegov**.

A ideia foi reunir, em um único projeto, os principais componentes estudados na disciplina:

- armazenamento de dados em camadas;
- processamento distribuído;
- ingestão e transformação;
- tabelas Delta Lake;
- modelagem dimensional;
- orquestração;
- testes de qualidade;
- catálogo e linhagem;
- visualização em dashboard;
- execução reproduzível com Docker.

A fonte utilizada foi o portal público do Transferegov:

- [API Pública / Dados Abertos do Transferegov](https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/)

Os quatro conjuntos de dados usados no projeto são:

```text
siconv_programa
siconv_programa_proposta
siconv_proposta
siconv_convenio
```

Também é utilizado o arquivo de controle:

```text
data_carga_siconv
```

para identificar se existe uma nova carga publicada pela fonte.

---

## 2. Metodologia usada para construir a arquitetura

O projeto foi desenvolvido de forma incremental. Em vez de instalar todas as tecnologias de uma vez, cada componente foi adicionado e validado antes da etapa seguinte.

A sequência metodológica foi:

```text
1. definir a fonte de dados
2. criar o ambiente com Docker Compose
3. configurar armazenamento no MinIO
4. configurar processamento com Apache Spark
5. adicionar Delta Lake
6. criar a ingestão da camada Bronze
7. criar Staging, Silver e Gold com dbt
8. criar a camada de consumo para o Superset
9. orquestrar o fluxo com Apache Airflow
10. adicionar testes, catálogo, linhagem e documentação
```

### 2.1 Princípios usados na containerização

Os containers foram organizados usando alguns princípios simples:

```text
um serviço por responsabilidade
        ↓
rede Docker compartilhada
        ↓
volumes persistentes
        ↓
dependências entre serviços
        ↓
healthchecks quando necessário
        ↓
portas expostas somente para os serviços usados no host
        ↓
Dockerfiles personalizados quando a imagem padrão não era suficiente
```

No projeto usamos dois tipos principais de armazenamento Docker:

- **named volumes**: para dados que devem continuar existindo mesmo quando o container é recriado;
- **bind mounts**: para disponibilizar o código do repositório dentro dos containers.

Exemplos de volumes persistentes usados:

```text
minio_data
airflow_db_data
thrift_metastore
```

Todos os serviços usam a rede:

```text
data-net
```

Assim, os containers conseguem conversar entre si usando o nome do serviço, por exemplo:

```text
minio:9000
spark-master:7077
spark-thrift-server:10000
airflow-db:5432
```

---

## 3. Arquitetura do projeto

### 3.1 Visão simples

```text
Transferegov
     ↓
RAW
     ↓
Bronze
     ↓
Silver
     ↓
Gold
     ↓
Serving
     ↓
Superset
```

### 3.2 Arquitetura lógica

```text
Transferegov (Dados Abertos)
     ↓
RAW (ZIP original + SHA-256)
     ↓
Bronze (Delta Lake, dados brutos + metadados técnicos)
     ↓
Staging (dbt ephemeral, tipagem e normalização)
     ↓
Silver (dados tipados e organizados)
     ↓
Gold Core (fatos, dimensões e bridge)
     ↓
Semantic View (contrato lógico para BI)
     ↓
Serving Mart (tabela física para consulta)
     ↓
Apache Superset
```

### 3.3 Arquitetura tecnológica

```text
                     Apache Airflow
                           │
                           │ orquestra
                           ▼
                    Apache Spark + dbt
                           │
             ┌─────────────┴─────────────┐
             │                           │
             ▼                           ▼
           MinIO                 Spark Thrift/Hive
      armazenamento                  catálogo SQL
             │                           │
             └─────────────┬─────────────┘
                           ▼
                       Superset
```

### 3.4 Visão dos containers

```text
                         Docker Compose
                               │
      ┌──────────────┬─────────┼──────────┬──────────────┐
      │              │         │          │              │
    MinIO        Spark Master  Airflow  PostgreSQL    Superset
      │              │         │          │
      │        ┌─────┴─────┐   │          │
      │        │           │   │          │
      │     Worker 1    Worker 2         │
      │            │                      │
      │       Spark Thrift/Hive           │
      │            │                      │
      └────────────┴────────── dbt ───────┘
```

---

## 4. Estrutura principal do repositório

```text
.
├── airflow/
│   ├── dags/
│   └── Dockerfile
├── dbt_lakehouse/
│   ├── models/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── spark/
│   ├── tests/
│   ├── Dockerfile
│   ├── ingest_transferegov.py
│   └── transferegov_sources.yml
├── superset/
│   └── Dockerfile
├── scripts/
├── docs/
├── docker-compose.yml
└── README.md
```

---

# 5. Pré-requisitos

Para reproduzir o projeto, é necessário ter:

- Git;
- Docker Desktop ou Docker Engine;
- Docker Compose v2;
- WSL2 no Windows, quando aplicável;
- espaço em disco para imagens, volumes e tabelas Delta.

Documentação oficial:

- [Docker Engine](https://docs.docker.com/engine/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Docker Desktop](https://docs.docker.com/desktop/)
- [Git](https://git-scm.com/doc)

Verifique a instalação:

```powershell
docker --version
docker compose version
git --version
```

Clone o repositório:

```powershell
git clone https://github.com/RaimundoMRNeto/lakehouse-transferencias-discricionarias-legais.git
cd lakehouse-transferencias-discricionarias-legais
```

---

# 6. Docker Compose

O arquivo central de infraestrutura é:

```text
docker-compose.yml
```

Ele define todos os serviços, redes, portas, volumes e dependências.

Os principais conceitos usados foram:

```yaml
services:
volumes:
networks:
ports:
environment:
depends_on:
healthcheck:
```

## 6.1 Construir as imagens

```powershell
docker compose build
```

## 6.2 Subir todos os serviços

```powershell
docker compose up -d
```

## 6.3 Verificar o estado

```powershell
docker compose ps
```

## 6.4 Ver os logs de um serviço

Exemplo:

```powershell
docker logs airflow --tail 50
```

ou:

```powershell
docker logs spark-master --tail 50
```

Documentação:

- [Docker Compose — documentação oficial](https://docs.docker.com/compose/)

---

# 7. MinIO — armazenamento de objetos

## 7.1 O que é

O MinIO é um armazenamento de objetos compatível com a API S3.

Neste projeto ele representa o armazenamento físico do Lakehouse.

Documentação:

- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## 7.2 Serviços usados

Foram criados dois containers:

```text
minio
minio-client (mc)
```

O primeiro executa o servidor. O segundo executa o cliente de linha de comando e cria os buckets.

## 7.3 Buckets criados

```text
bronze
silver
gold
```

A organização principal é:

```text
bronze/
├── raw/
│   └── transferegov/
└── warehouse/

silver/
└── warehouse/

gold/
└── warehouse/
```

Os arquivos ZIP originais ficam em:

```text
s3://bronze/raw/transferegov/<dataset>/
```

As tabelas Bronze ficam, por exemplo, em:

```text
s3a://bronze/warehouse/siconv_proposta
s3a://bronze/warehouse/siconv_convenio
```

## 7.4 Verificar os buckets

```powershell
docker exec mc mc ls local
```

Visualizar a Bronze:

```powershell
docker exec mc mc ls local/bronze
```

Listar recursivamente:

```powershell
docker exec mc mc ls --recursive local/bronze
```

## 7.5 Interface web

```text
http://localhost:9001
```

> As credenciais existentes no `docker-compose.yml` são próprias do ambiente acadêmico/local. Elas não devem ser reutilizadas em produção.

---

# 8. Apache Spark — processamento distribuído

## 8.1 O que é

O Apache Spark é o mecanismo usado para processamento dos dados.

Documentação:

- [Apache Spark 3.4.3](https://spark.apache.org/docs/3.4.3/)

## 8.2 Estrutura usada

O cluster local possui:

```text
spark-master
├── spark-worker-1
└── spark-worker-2
```

Cada worker foi configurado com:

```text
2 cores
2 GB de memória
```

O endereço interno do master é:

```text
spark://spark-master:7077
```

## 8.3 Verificar os containers

```powershell
docker logs spark-master --tail 50
docker logs spark-worker-1 --tail 50
docker logs spark-worker-2 --tail 50
```

## 8.4 Interface web

```text
http://localhost:8081
```

Nessa tela é possível verificar os workers conectados ao master.

---

# 9. Delta Lake — tabelas transacionais

## 9.1 O que é

Delta Lake adiciona recursos transacionais às tabelas armazenadas no Data Lake/Lakehouse.

Documentação:

- [Delta Lake Documentation](https://docs.delta.io/)

Neste projeto usamos:

```text
Spark 3.4.3
Delta Lake 2.4.0
Hadoop AWS 3.3.4
```

Os JARs necessários são instalados no arquivo:

```text
spark/Dockerfile
```

Entre eles:

```text
hadoop-aws
aws-java-sdk-bundle
delta-core
delta-storage
```

## 9.2 Estrutura de uma tabela Delta

Ao abrir uma tabela no MinIO, é normal encontrar:

```text
tabela_delta/
├── _delta_log/
├── part-00000-....parquet
├── part-00001-....parquet
└── ...
```

Os arquivos Parquet guardam os dados e `_delta_log` guarda o histórico transacional.

---

# 10. Spark Thrift Server e Hive Metastore

## 10.1 Por que são necessários

O MinIO guarda os arquivos, mas ferramentas como dbt e Superset precisam consultar os dados como tabelas SQL.

Por isso usamos:

```text
MinIO
  ↓
Delta Lake
  ↓
Hive Metastore
  ↓
Spark Thrift Server
  ↓
dbt / Superset
```

O serviço é:

```text
spark-thrift-server
```

A porta SQL é:

```text
10000
```

## 10.2 Persistência do catálogo

Foi criado o volume:

```text
thrift_metastore
```

Ele evita perder o catálogo quando o container é recriado.

## 10.3 Teste simples via PyHive

O container do Airflow já possui PyHive. Para listar os bancos:

```powershell
docker exec airflow python -c "from pyhive import hive; c=hive.Connection(host='spark-thrift-server', port=10000, username='airflow'); cur=c.cursor(); cur.execute('SHOW DATABASES'); print(cur.fetchall())"
```

Esperado: aparecerem schemas como `bronze`, `silver` e `gold`.

---

# 11. Apache Airflow e PostgreSQL

## 11.1 O que é

O Airflow é o orquestrador do projeto.

Documentação:

- [Apache Airflow 2.9.1](https://airflow.apache.org/docs/apache-airflow/2.9.1/)

Foram usados dois serviços:

```text
airflow-db
airflow
```

O PostgreSQL armazena os **metadados do Airflow**, como:

- DAG Runs;
- Task Instances;
- usuários;
- logs de estado;
- histórico operacional.

Ele não armazena as tabelas analíticas do Lakehouse.

## 11.2 Imagem personalizada

O arquivo:

```text
airflow/Dockerfile
```

parte de:

```dockerfile
FROM apache/airflow:2.9.1
```

e instala:

```text
Java 17
PySpark 3.4.3
dbt-core 1.10.9
dbt-spark 1.9.3
PyHive
psycopg2
```

## 11.3 Inicialização

Durante a subida do container são executados:

```bash
airflow db migrate
airflow scheduler
airflow webserver
```

## 11.4 DAGs do projeto

O projeto usa três DAGs principais:

```text
r2_ingestao_transferegov_bronze
r6_transformacoes_lakehouse
r6_pipeline_transferegov_e2e
```

A DAG E2E coordena as duas demais.

Fluxo simplificado:

```text
r6_pipeline_transferegov_e2e
        ↓
r2_ingestao_transferegov_bronze
        ↓
NO_CHANGE ────────────────→ fim
        │
        └── SUCCESS
              ↓
r6_transformacoes_lakehouse
              ↓
Silver → Gold → Semantic → Serving
```

## 11.5 Listar DAGs

```powershell
docker exec airflow airflow dags list
```

## 11.6 Executar o pipeline completo

```powershell
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e
```

Forçar uma nova ingestão:

```powershell
docker exec airflow airflow dags trigger -c '{"force_bronze": true}' r6_pipeline_transferegov_e2e
```

## 11.7 Interface web

```text
http://localhost:8080
```

---

# 12. dbt — transformação, testes e documentação

## 12.1 O que é

O dbt organiza as transformações SQL do Lakehouse.

Documentação:

- [dbt Core](https://docs.getdbt.com/docs/core)
- [dbt Spark](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup)

O projeto dbt está em:

```text
dbt_lakehouse/
```

## 12.2 Conexão com Spark

O arquivo:

```text
dbt_lakehouse/profiles.yml
```

usa:

```yaml
type: spark
method: thrift
host: spark-thrift-server
port: 10000
```

Assim:

```text
dbt
 ↓
Spark Thrift Server
 ↓
Delta Lake
 ↓
MinIO
```

## 12.3 Camadas dbt

```text
staging
silver
gold
semantic
serving
```

### Staging

Modelos temporários (`ephemeral`) usados para:

- conversão de tipos;
- limpeza de strings;
- conversão de datas;
- conversão de valores monetários.

### Silver

Tabelas tipadas e padronizadas.

### Gold

Modelo dimensional com:

- fatos;
- dimensões;
- bridge N:N.

### Semantic

View preparada para consumo analítico.

### Serving

Tabela física usada pelo Superset.

## 12.4 Testar a conexão

```powershell
docker exec airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt debug --profiles-dir ."
```

## 12.5 Validar o projeto

```powershell
docker exec airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt parse --profiles-dir ."
```

## 12.6 Gerar a documentação

```powershell
docker exec airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt docs generate --no-partial-parse --profiles-dir ."
```

Servir a documentação:

```powershell
docker exec -d airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt docs serve --host 0.0.0.0 --port 8091 --profiles-dir ."
```

Acesso:

```text
http://localhost:8091
```

## 12.7 Linhagem

O dbt Docs permite visualizar a linhagem:

```text
Sources Bronze
      ↓
Staging
      ↓
Silver
      ↓
Gold
      ↓
Semantic
      ↓
Serving
```

---

# 13. Apache Superset — visualização dos dados

## 13.1 O que é

O Apache Superset é usado para criar gráficos, filtros e dashboards.

Documentação:

- [Apache Superset](https://superset.apache.org/docs/intro)
- [Superset com Docker Compose](https://superset.apache.org/docs/installation/docker-compose)

O arquivo:

```text
superset/Dockerfile
```

instala dependências para comunicação com Hive/Spark Thrift.

## 13.2 Fluxo da consulta

```text
Serving Mart
      ↓
Spark Thrift Server
      ↓
Apache Superset
```

A conexão usada no projeto é baseada em Hive/PyHive:

```text
hive://spark-thrift-server:10000/gold
```

## 13.3 Passos para configurar

Após subir o container:

1. acessar o Superset;
2. cadastrar a conexão com Spark/Hive;
3. selecionar o schema `gold`;
4. cadastrar `mart_superset_proposta_convenio` como dataset;
5. criar métricas;
6. criar gráficos;
7. criar filtros;
8. montar o dashboard.

## 13.4 Interface web

```text
http://localhost:8088
```

---

# 14. Fonte Transferegov e ingestão Bronze

A configuração da fonte está em:

```text
spark/transferegov_sources.yml
```

Ela contém:

- URL oficial;
- arquivos ZIP;
- nomes dos CSVs;
- paths Delta;
- paths RAW;
- opções de CSV;
- configuração Spark;
- retenção.

A URL-base é:

```text
https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/
```

Exemplo:

```yaml
- id: siconv_proposta
  zip_file: siconv_proposta.zip
  member_file: siconv_proposta.csv
  delta_path: s3a://bronze/warehouse/siconv_proposta
  raw_s3_prefix: s3://bronze/raw/transferegov/siconv_proposta
```

O script principal é:

```text
spark/ingest_transferegov.py
```

Ele executa, de forma resumida:

```text
download
   ↓
SHA-256
   ↓
validação ZIP/CSV
   ↓
RAW no MinIO
   ↓
Delta Bronze
   ↓
registro de auditoria
```

---

# 15. Git e GitHub Actions

## 15.1 Metodologia de versionamento

O desenvolvimento foi organizado com branches de funcionalidade:

```text
main
  ↓
feature branch
  ↓
testes locais
  ↓
push
  ↓
Pull Request
  ↓
GitHub Actions
  ↓
merge
```

Documentação:

- [GitHub Actions](https://docs.github.com/actions)
- [GitHub Pull Requests](https://docs.github.com/pull-requests)

O workflow está em:

```text
.github/workflows/ci.yml
```

## 15.2 O que o CI verifica

```text
whitespace
sintaxe Python
YAML do Transferegov
testes unitários
contrato de governança
dbt parse
```

---

# 16. Como executar o projeto do início ao fim

## 16.1 Clonar

```powershell
git clone https://github.com/RaimundoMRNeto/lakehouse-transferencias-discricionarias-legais.git
cd lakehouse-transferencias-discricionarias-legais
```

## 16.2 Construir

```powershell
docker compose build
```

## 16.3 Subir

```powershell
docker compose up -d
```

## 16.4 Verificar

```powershell
docker compose ps
```

## 16.5 Executar o pipeline

```powershell
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e
```

## 16.6 Acompanhar no Airflow

```text
http://localhost:8080
```

## 16.7 Abrir o MinIO

```text
http://localhost:9001
```

## 16.8 Abrir o Superset

```text
http://localhost:8088
```

## 16.9 Gerar dbt Docs

```powershell
docker exec airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt docs generate --no-partial-parse --profiles-dir ."
docker exec -d airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt docs serve --host 0.0.0.0 --port 8091 --profiles-dir ."
```

Acesso:

```text
http://localhost:8091
```

---

# 17. Como verificar se tudo funcionou

## Docker

```powershell
docker compose ps
```

Todos os containers principais devem estar em execução.

## MinIO

```powershell
docker exec mc mc ls local
```

Esperado:

```text
bronze
silver
gold
```

## Spark

Abra:

```text
http://localhost:8081
```

e verifique os dois workers.

## Airflow

```powershell
docker exec airflow airflow dags list
```

As três DAGs principais devem aparecer.

## dbt

```powershell
docker exec airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt debug --profiles-dir ."
```

Esperado: conexão válida com o Spark Thrift Server.

## Superset

Abra:

```text
http://localhost:8088
```

e verifique se o dashboard carrega os indicadores e gráficos.

---

# 18. Endpoints locais

| Serviço | Endereço | Uso |
|---|---|---|
| Airflow | `http://localhost:8080` | Orquestração |
| MinIO Console | `http://localhost:9001` | Armazenamento |
| Spark Master | `http://localhost:8081` | Cluster Spark |
| Superset | `http://localhost:8088` | Dashboard |
| dbt Docs | `http://localhost:8091` | Catálogo e linhagem |
| Spark Thrift | `localhost:10000` | SQL/Thrift |
| MinIO API | `localhost:9000` | S3 compatível |

---

# 19. Governança, qualidade e linhagem

Além de executar as transformações, o projeto também registra e verifica a qualidade dos dados.

Entre os controles implementados estão:

- SHA-256 dos arquivos oficiais;
- controle de `data_carga_siconv`;
- `ingestion_run_id`;
- `bronze.ingestion_runs`;
- `bronze.ingestion_manifest`;
- testes `not_null`;
- testes `unique`;
- testes `relationships`;
- testes `accepted_values`;
- reconciliação Bronze → Silver;
- reconciliação Silver → Gold;
- testes de equivalência do Serving;
- dbt Docs e Lineage Graph.

A relação Programa × Proposta é N:N. Por isso, a tabela:

```text
bridge_programa_proposta
```

não deve ser usada para somar valores financeiros diretamente.

O campo:

```text
valor_saldo_conta
```

também é tratado como observacional e não é usado como métrica aditiva no dashboard.

Documentação detalhada:

- [Governança de Dados](docs/governanca/README.md)
- [Catálogo e Contratos](docs/governanca/catalogo_contratos.md)
- [Qualidade e Linhagem](docs/governanca/qualidade_linhagem.md)
- [Operação e Reprodutibilidade](docs/governanca/operacao_reprodutibilidade.md)

---

# 20. Como parar o ambiente

Parada normal:

```powershell
docker compose down
```

> **Atenção:** não use `docker compose down -v` se quiser preservar os dados.

O parâmetro `-v` remove os volumes persistentes e pode apagar:

- dados do MinIO;
- metadados do Airflow;
- metastore Hive/Thrift.

---

# 21. Referências técnicas e documentação oficial

## Curso e disciplina

- [UTFPR — Especialização em Ciência de Dados](https://www.utfpr.edu.br/cursos/especializacao/dv/ciencia-de-dados)
- [UTFPR — Ementa de Arquitetura de Dados](https://coens.dv.utfpr.edu.br/pos/ciencia-dados/disciplinas.html)

## Dados

- [Transferegov — Dados Abertos](https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/)

## Infraestrutura

- [Docker](https://docs.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [MinIO](https://min.io/docs/minio/linux/index.html)

## Processamento e armazenamento

- [Apache Spark 3.4.3](https://spark.apache.org/docs/3.4.3/)
- [Delta Lake](https://docs.delta.io/)

## Orquestração

- [Apache Airflow 2.9.1](https://airflow.apache.org/docs/apache-airflow/2.9.1/)

## Transformação e catálogo

- [dbt Core](https://docs.getdbt.com/docs/core)
- [dbt Spark](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup)

## Visualização

- [Apache Superset](https://superset.apache.org/docs/intro)
- [Superset — Docker Compose](https://superset.apache.org/docs/installation/docker-compose)

## Versionamento e CI

- [Git](https://git-scm.com/doc)
- [GitHub Actions](https://docs.github.com/actions)
- [Pull Requests](https://docs.github.com/pull-requests)

---

# 22. Observação sobre o ambiente

Este projeto foi construído para **fins acadêmicos e execução local**.

A arquitetura demonstra como integrar diferentes componentes de uma plataforma de dados moderna, mas não deve ser considerada uma implantação pronta para produção sem adaptações adicionais, como:

- gerenciamento externo de segredos;
- autenticação e autorização;
- monitoramento centralizado;
- alta disponibilidade;
- backup;
- políticas corporativas de segurança;
- escalabilidade de infraestrutura.
