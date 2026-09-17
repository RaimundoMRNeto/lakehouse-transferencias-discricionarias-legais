# R4-A.1 — Evidência de Execução Validada

## Objetivo

Registrar a evidência operacional válida da checagem final de conformação de `dim_orgao` no ambiente real do projeto, após a revisão humana do contrato analítico da Gold.

Este documento complementa os artefatos do R4-A/R4-A.1 e substitui, para fins de evidência operacional, qualquer síntese externa que tenha mencionado containers, versões ou tecnologias não pertencentes ao stack deste repositório.

## Ambiente efetivamente utilizado

- Container executor: `airflow`
- Script: `/app/profile_r4a.py`
- Acesso SQL: PyHive
- Endpoint: `spark-thrift-server:10000`
- Engine: Apache Spark 3.4.3
- Formato de armazenamento: Delta Lake
- Catálogo: Spark/Hive Thrift Server

Não foram utilizados `spark-iceberg`, Spark 3.5, Apache Iceberg ou `spark-submit` nesta validação.

## Comando executado

```powershell
docker exec airflow python -u /app/profile_r4a.py --section orgao
```

Execução realizada em 17/09/2026.

## Resultado observado

A seção `orgao` iniciou e concluiu com sucesso, executando as três dependências funcionais locais e as três comparações cruzadas entre papéis institucionais.

Resumo final emitido pelo profiler:

```text
[RESULT] orgao: conformed_safe=True, sup=37, conc=182, inter_conc_prog=37, diffs_conc_prog=0
[END] section=orgao (duracao: 21.80s)
[END] Profiling R4-A concluido com sucesso.
```

Evidências principais:

- códigos de órgão superior em proposta: `37`;
- códigos de órgão concedente em proposta: `182`;
- códigos compartilhados entre órgão concedente de proposta e órgão superior de programa: `37`;
- divergências de descrição no terceiro par: `0`;
- `conformed_dim_orgao_defensible = True`.

A lógica versionada exige simultaneamente zero divergências nos três pares:

1. órgão superior da proposta ↔ órgão concedente da proposta;
2. órgão superior da proposta ↔ órgão superior do programa;
3. órgão concedente da proposta ↔ órgão superior do programa.

Com os três pares sem divergências, permanece validada a decisão de uma dimensão única conformada `gold.dim_orgao` com role-playing.

## Nomenclatura canônica para o R4-B

As chaves e grãos canônicos permanecem os definidos nos documentos do contrato, sem as derivações incorretas que apareceram em uma síntese externa da execução:

| Entidade | Chave / grão canônico |
|---|---|
| `gold.dim_proponente` | PK natural `identificacao_proponente` |
| `gold.dim_municipio` | PK natural `codigo_municipio_ibge` (7 dígitos) |
| `gold.dim_orgao` | PK natural `codigo_orgao` |
| `gold.dim_programa` | PK natural `id_programa` |
| `gold.fct_proposta` | PK `id_proposta` |
| `gold.fct_convenio` | PK natural `numero_convenio` |
| `gold.bridge_programa_proposta` | PK composta `(id_programa, id_proposta)` |
| `gold.fct_convenio_saldo_observacao` | PK `id_convenio_observacao` |

`dim_municipio` é derivada do universo observado em `silver.siconv_proposta`; os 5.570 códigos municipais distintos observados não constituem, por si só, prova de cobertura oficial nacional externa.

## Status do gate operacional

```text
R4-A.1 — terceira comparação de órgão      PASS
Ambiente real do projeto                    PASS
Conformação de dim_orgao                    PASS
Nomenclatura canônica                       VALIDADA
Gold materializada                          NÃO
```

Com esta evidência, o bloqueio operacional remanescente da revisão humana do R4-A.1 é considerado encerrado. A implementação do R4-B continua condicionada à autorização humana explícita e às regras do contrato analítico versionado.
