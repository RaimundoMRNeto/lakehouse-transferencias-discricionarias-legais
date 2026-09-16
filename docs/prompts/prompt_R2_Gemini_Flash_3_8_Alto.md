# Gemini Flash 3.8 — Alto | Implementação do R2 no Antigravity

## 1. Missão e autonomia

Implemente o R2 — Ingestão/Bronze do projeto acadêmico Lakehouse de Transferências Discricionárias e Legais. Produza código, execute testes e documente as evidências. Não entregue apenas um plano.

Repositório: RaimundoMRNeto/lakehouse-transferencias-discricionarias-legais
Diretório: C:\Dev\lakehouse-transferencias-discricionarias-legais
Branch esperada: feat/r2-ingestao-bronze

Você está autorizado a editar arquivos deste projeto, executar testes, baixar as fontes públicas especificadas e reconstruir somente os serviços necessários. Não peça confirmação a cada edição. Antes de cada bloco de comandos, explique brevemente a finalidade e os efeitos; depois informe o resultado observado. O usuário também está aprendendo Engenharia de Dados.

Apesar da permissão Full machine da IDE, não altere outros repositórios, arquivos pessoais ou configurações globais. Git não recupera automaticamente volumes Docker ou arquivos não versionados.

Não faça merge na main, push, force-push, reset destrutivo, limpeza global Docker ou remoção de volumes. Não execute VACUUM. Deixe as alterações para revisão final, sem commit automático. A limpeza específica de temporários e a retenção RAW descritas abaixo estão autorizadas, após testes e respeitando seus limites.

## 2. Diagnóstico inicial obrigatório

Antes de editar:

- Confirme diretório, branch, HEAD, remotes e git status. Preserve alterações preexistentes. Se a branch não for a esperada ou houver conflito real com trabalho existente, pare e explique.
- Leia README, docker-compose.yml, Dockerfiles, .gitignore, .gitattributes, profiles.yml e dbt_project.yml. Respeite instruções de projeto existentes, quando aplicáveis.
- Confira containers, montagens, versões efetivas, conectividade, permissões de escrita e espaço livre para ZIPs, CSVs, Delta e retenção.
- Registre um plano curto de implementação e prossiga, sem aguardar aprovação de rotina.

O R1 foi validado pelo usuário com Airflow 2.9.1, Spark 3.4.3, Delta Lake 2.4.0, dbt-core 1.10.9 e dbt-spark 1.9.3. Há MinIO, PostgreSQL do Airflow, Spark Master, dois workers, Thrift Server e Superset: nove serviços. O Superset usa uma imagem de desenvolvimento; não o atualize neste marco.

Confirme isso no ambiente, sem tratar o histórico como substituto da inspeção. O R1 provou conexão SQL e funcionamento dos serviços, mas ainda não provou ingestão distribuída dos dados reais.

Evite atualizações de versões. Dependência adicional indispensável deve ser declarada na imagem/configuração, com versão explícita e validação de compatibilidade, não instalada apenas no container em execução.

## 3. Escopo funcional

Entregar:

Fonte oficial → ZIP RAW preservado → CSV temporário → Spark → Delta Bronze → catálogo Spark Thrift → auditoria.

Somente quatro datasets analíticos e uma fonte técnica de controle. Ingerir os snapshots nacionais completos disponibilizados desses quatro datasets.

Não implementar Silver, Gold, recorte por órgão/período/tema, joins, deduplicação, conversões financeiras, dashboard ou DataHub. Não incorporar SEI, scraping autenticado ou dependência operacional do SIAGIR. MDA não faz parte do nome nem da regra da Bronze.

## 4. Fontes oficiais centralizadas

Base:
https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/

Dataset → arquivo ZIP → membro CSV → tabela:

- siconv_programa → siconv_programa.zip → siconv_programa.csv → bronze.siconv_programa
- siconv_programa_proposta → siconv_programa_proposta.zip → siconv_programa_proposta.csv → bronze.siconv_programa_proposta
- siconv_proposta → siconv_proposta.zip → siconv_proposta.csv → bronze.siconv_proposta
- siconv_convenio → siconv_convenio.zip → siconv_convenio.csv → bronze.siconv_convenio

Controle:
https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/data_carga_siconv.zip
Membro esperado: data_carga_siconv.csv.

Centralize essas definições em spark/transferegov_sources.yml. Verifique acesso e estrutura real. Não substitua endpoints por mirrors ou pela fonte antiga silenciosamente.

## 5. Controle por data_carga

Inspecione o ZIP de controle. O formato esperado, conforme a referência do SIAGIR, é um CSV com coluna data_carga e um registro no formato DD/MM/AAAA HH:MM:SS. Valide estrutura e data calendarizável; documente divergências antes de adaptar o parser.

Diferencie:

- source_data_carga_raw: texto publicado, preservado.
- source_data_carga: representação parseada sem inventar fuso horário.
- retrieved_at_utc: instante em que nosso download terminou.
- ingested_at_utc: instante da incorporação à Bronze.
- source_last_modified e etag: metadados HTTP opcionais, não equivalentes a data_carga ou SHA-256.

Não atribua UTC ou America/Sao_Paulo à data oficial sem evidência. Use UTC explicitamente nos horários produzidos pelo pipeline.

Parâmetro de execução: force=false por padrão.

Com force=false, só registre NO_CHANGE e dispense os quatro downloads quando a data declarada for igual à da última execução completa bem-sucedida E o estado local esperado estiver íntegro: quatro tabelas legíveis, referências coerentes, RAW atual presente e nenhuma publicação parcial pendente. Primeira carga ou estado incompleto exige processamento.

Com force=true, baixe novamente e recalcule hashes, mesmo com data_carga igual. Isso não obriga reescrita de Delta idêntico e válido.

O marcador oficial é uma otimização, não prova absoluta de igualdade dos arquivos. NO_CHANGE sem download não deve ser descrito como verificação de hashes remotos.

Leia data_carga antes e depois de uma carga efetiva. Se mudar durante a operação, registre a inconsistência, não avance o marcador de sucesso e não execute retenção. Não entre em repetição ilimitada. Marcadores iguais reduzem esse risco, mas não comprovam publicação atômica dos quatro arquivos pela fonte.

Falha ou conteúdo inválido do controle não significa ausência de atualização: a execução deve falhar explicitamente.

## 6. Download e armazenamento

Implemente download HTTP por blocos, com timeout, tentativas limitadas, backoff, verificação de status e TLS habilitado. Calcule SHA-256 durante a leitura. Use arquivo .part e só promova após validação.

Verifique ZIP, integridade do conteúdo, membro esperado, tamanho e espaço disponível. Extraia somente o membro previsto para caminho controlado; não use extração irrestrita. Não carregue ZIP/CSV inteiro em memória.

Áreas temporárias:
/data/landing/<execucao>/<dataset>/
/data/staging/<execucao>/<dataset>/

Valide os identificadores utilizados nos caminhos. Aproveite as montagens existentes ./data:/data; os caminhos precisam ser acessíveis também aos executores Spark que os lerem.

Preserve o ZIP original, byte a byte, em:
s3://bronze/raw/transferegov/<dataset>/sha256=<hash>/<arquivo>.zip

Aplique a mesma organização ao ZIP de controle. Use um cliente S3/MinIO compatível, confirmando envio e integridade. Não interprete ETag como SHA-256.

Um caminho baseado em hash não garante imutabilidade sozinho: o código deve impedir sobrescrita com bytes diferentes. Registre divergências.

Destino das quatro tabelas:
s3a://bronze/warehouse/<dataset>

Não versione datasets, temporários, credenciais, logs extensos ou caches. Após sucesso, remova apenas os temporários pertencentes à execução. Em falha, remova parciais inválidos e preserve diagnóstico mínimo, sem acumulação indefinida nem limpeza de diretórios genéricos.

## 7. Parsing fiel da Bronze

Preserve nomes e ordem das colunas oficiais, registros repetidos, zeros à esquerda, espaços relevantes e valores textuais. Campos oficiais devem ser StringType. Não aplique inferência de datas, valores monetários ou identificadores.

Inspecione delimitador, BOM, encoding, aspas, escapes e campos multilinha. Uma amostra pode orientar a configuração, mas a decodificação do arquivo inteiro deve ser validada. Não substitua bytes inválidos silenciosamente nem adote Latin-1 como solução universal.

Rejeite headers vazios, duplicados, incompatíveis sem tratamento explícito ou que colidam com metadados técnicos. Detecte e registre mudanças de schema; não use overwriteSchema=true indiscriminadamente para aceitar qualquer mudança.

Configure explicitamente o tratamento de strings vazias, NULL literal e campos ausentes. Não permita conversão silenciosa de texto para nulo. O ZIP RAW é a evidência exata dos bytes; Delta é sua representação tabular, não uma cópia byte a byte.

Valide quantidade de campos por registro e registros malformados. Não use DROPMALFORMED. FAILFAST sozinho não substitui a validação de largura dos registros.

Conte registros CSV lógicos, respeitando aspas e quebras de linha internas; não conte linhas físicas. Compare uma contagem estrutural da entrada com a tabela Delta efetivamente gravada. Evite uma validação circular baseada apenas em duas contagens do mesmo DataFrame.

Validação em streaming é permitida; processamento tabular fica no Spark. Não use Pandas integral, collect() integral ou listas contendo todos os registros no driver.

Acrescente apenas:
__ingested_at_utc
__ingestion_run_id
__source_file
__source_sha256

Esses campos técnicos podem ser tipados. Não altere esses valores em uma revisão reaproveitada apenas para fazê-la parecer recém-ingerida.

## 8. Spark e catálogo

Use o cluster existente, com recursos conservadores e uma ingestão por vez. Não mude para local[*] silenciosamente. Verifique caminhos compartilhados, comunicação driver/executores, JARs S3A/Delta e compatibilidade Python quando houver execução Python nos workers. Evite UDFs desnecessárias.

Reutilize o Thrift em spark-thrift-server:10000. Não tente abrir o Derby do Thrift diretamente em outra sessão.

Crie o schema bronze com LOCATION explícita em s3a://bronze/warehouse. Registre tabelas externas Delta com LOCATION correto. Não deixe o warehouse padrão do Thrift direcionar a Bronze ao bucket gold.

Verifique registros existentes antes de alterá-los. Não faça DROP/CREATE indiscriminadamente. Atualize metadados/cache quando necessário e valide leitura via Thrift, não apenas na sessão que escreveu.

A Bronze disponibiliza o snapshot lógico atual de cada dataset. Novos conteúdos usam overwrite transacional Delta, sem apagar diretórios manualmente. Overwrite não equivale a remoção física imediata das versões antigas.

## 9. Idempotência e recuperação

Compare o SHA baixado com o SHA do snapshot ATUAL válido, não apenas com qualquer hash encontrado no histórico.

- Mesmo SHA atual + Delta/RAW válidos: reaproveitar, sem acrescentar linhas ou regravar tabela.
- SHA novo: ingerir e validar novo snapshot.
- SHA já visto, mas diferente do atual: tratar como nova ativação, não pular incorretamente.
- Estado ausente ou inválido: recuperar/reprocessar; nunca registrar SKIPPED apenas porque existe uma linha antiga no manifesto.

Tentativas repetidas da mesma tarefa não podem duplicar registros de auditoria. Use chaves determinísticas por execução/dataset e atualizações idempotentes.

Valide entrada antes de sobrescrever. Se houver falha após atualização de algumas tabelas, registre publicação parcial, versões afetadas e recuperação necessária. Não avance data_carga nem aplique retenção.

Não prometa atomicidade entre quatro tabelas Delta e tabelas de controle. Para este projeto, mantenha processamento serial, estado de execução explícito e reexecução recuperável. A leitura analítica futura só poderá confiar em uma execução globalmente validada. Não construa um framework de transações distribuídas.

## 10. Auditoria persistente

Implemente duas tabelas técnicas Delta no bucket Bronze, também consultáveis via Thrift:

bronze.ingestion_runs
Uma linha por ingestion_run_id. Registrar data_carga inicial/final e textos originais, identidade do ZIP de controle, início/fim UTC, force, situação global, totais de datasets, erro resumido e resultado separado da retenção.

Estados mínimos: RUNNING, SUCCESS, FAILED e NO_CHANGE.

bronze.ingestion_manifest
Uma linha por (ingestion_run_id, dataset_id). Registrar URL solicitada/efetiva, arquivo/membro, SHA-256, tamanho, horário de obtenção, encoding/delimitador, colunas, contagem de entrada/saída, RAW key, Delta path/versão efetiva, duração, situação e motivo de reaproveitamento/falha. Referenciar a carga original quando reutilizada.

Preservar histórico de execuções e proveniência mesmo após descarte físico de ZIPs. Registrar exclusão confirmada, horário e motivo, sem alterar a identidade original da fonte. Manter um estado atual de retenção por objeto que não apresente o mesmo hash simultaneamente como disponível e apagado.

Uma falha de infraestrutura pode impedir a gravação no manifesto: nesse caso, retornar erro não zero e manter evidência nos logs do Airflow. Não converter exceção em falso SUCCESS.

## 11. Retenção: atual + anterior

Configuração:
retention.raw_versions_per_dataset: 2

Após carga COMPLETA e validada, manter por dataset:
1. revisão atualmente ativa;
2. revisão distinta imediatamente anterior ativada com sucesso.

O ZIP data_carga segue a mesma política. Reexecução do mesmo conteúdo não conta como versão nova. Ordene pela ativação bem-sucedida, não pelo nome do hash ou mera data de upload.

Antes de apagar, calcule e registre o plano. Exclua somente chaves RAW exatas, gerenciadas por este pipeline e fora das duas revisões protegidas. Nunca faça limpeza recursiva genérica do bucket. Proteja cargas em andamento e revisões necessárias à recuperação.

Retenção não deve rodar após carga parcial/falha. Pode haver temporariamente mais de dois ZIPs enquanto a nova carga é validada. Trate candidatos falhos identificados sem remover revisões válidas.

Se a limpeza falhar, preserve o sucesso dos dados separadamente, marque a retenção pendente/falha e permita repetir só a limpeza. Não declare objeto apagado antes de confirmar a exclusão.

Inclua dry-run da retenção e teste com três revisões sintéticas antes de habilitar remoções reais.

Não executar VACUUM nem apagar Parquet/_delta_log manualmente. Documentar que duas versões RAW não limitam o armazenamento físico Delta a duas versões; a manutenção Delta fica fora do R2.

## 12. DAG e organização do código

DAG: r2_ingestao_transferegov_bronze
schedule=None; catchup=False; max_active_runs=1; force booleano, false por padrão.

Fluxo principal:
preflight → controle inicial → decisão → quatro ingestões sequenciais → controle final → validação global → registro de sucesso → retenção.

O caminho sem mudança deve registrar NO_CHANGE. Uma tarefa que reaproveite dataset não pode impedir indevidamente a execução dos demais. Diferencie o status SKIPPED do manifesto do estado skipped de uma tarefa Airflow.

Não baixe arquivos, abra Spark ou consulte serviços durante importação da DAG. Use XCom apenas para metadados pequenos, nunca datasets. Garanta que tarefas finais de limpeza/auditoria não escondam uma falha anterior da DAG.

Não introduza scheduler paralelo, Docker socket no Airflow ou novos serviços. Não execute duas cargas concorrentes por atalhos fora da DAG.

Estrutura sugerida, ajustável de forma pequena e justificada:

spark/ingest_transferegov.py
spark/transferegov_sources.yml
spark/transferegov/                 # módulos pequenos, se necessário
spark/tests/
airflow/dags/r2_ingestao_transferegov_bronze.py
docs/r2_ingestao_bronze.md
docs/r2_relatorio_validacao.md

Atualize README e dependências/configurações indispensáveis. Não crie arquitetura empresarial, interfaces abstratas sem uso ou arquivos gigantes. Use funções claras, comentários úteis, tratamento de erros e parâmetros centralizados. Credenciais novas devem vir de configuração local/variáveis de ambiente, não de valores reais incluídos no código.

## 13. Testes e execução real

Primeiro execute testes pequenos, isolados da Bronze real, cobrindo:

- data_carga válida, ausente, inválida, igual, alterada e force;
- ZIP inválido, membro ausente e download interrompido;
- encoding, BOM, aspas, multilinha, zeros à esquerda, vazio, NULL literal e largura incorreta;
- headers inválidos e colisão com prefixos técnicos;
- contagem de registros e preservação das colunas;
- idempotência e recuperação após falha parcial;
- retenção A → B → C, mantendo B e C, e reaproveitamento de hash;
- nenhuma exclusão em falha e dry-run sem efeitos;
- DAG importável, caminho NO_CHANGE e propagação correta de falhas.

Use fixtures sintéticas pequenas e namespaces de teste separados. Nunca altere artificialmente os datasets oficiais para simular versões.

Depois, execute uma carga real completa dos quatro datasets pela DAG. Valide RAW, hashes, schema, contagens, manifesto e consultas via Thrift.

Reexecute normalmente: quando data_carga permanecer igual e o estado estiver íntegro, espere NO_CHANGE sem downloads grandes. Faça também uma verificação forçada controlada para confirmar reaproveitamento por hash. Se a fonte mudar entre execuções, registre o fato e não force artificialmente o resultado esperado.

Evite downloads repetidos durante depuração e execuções concorrentes. Reaproveite artefatos previamente verificados quando apropriado.

Revalide os serviços do R1 afetados e dbt debug. Avalie uso de disco e memória. Se um teste não puder ser executado, apresente causa e limite da evidência; não o marque como aprovado.

## 14. Critérios de aceite e relatório final

R2 somente poderá ser declarado tecnicamente aprovado com:

- quatro ZIPs oficiais e controle tratados corretamente;
- quatro tabelas Delta Bronze completas e consultáveis via Thrift;
- campos oficiais textuais, sem filtro/deduplicação/regra de negócio;
- contagens de entrada e saída reconciliadas;
- auditoria persistente de execuções e datasets;
- controle data_carga, force e hashes testados;
- reexecução sem duplicação e falhas sem falso sucesso;
- retenção atual + anterior testada com exclusão restrita;
- DAG manual funcional e sem erros de importação;
- R1 preservado, documentação atualizada e diff revisável;
- nenhum dado bruto ou segredo novo incluído no Git.

No relatório final, apresente:

1. Branch, HEAD inicial e arquivos criados/alterados.
2. Explicação do fluxo implementado e eventuais ajustes mínimos.
3. Execuções reais: run_id, data_carga, hashes, tamanhos, contagens e estados por dataset.
4. Testes executados, resultados e testes não executados.
5. Resultado da repetição normal, force, retenção e verificação do R1.
6. Uso de armazenamento observado, limitações e pendências.
7. Comandos para o usuário repetir a validação, cada um explicado.
8. git diff --check, resumo do diff, git status e sugestão de mensagem de commit.
9. Parecer: APROVADO, APROVADO COM RESSALVAS ou BLOQUEADO, sustentado pelas evidências.

Não chame teste com fixture de carga real, container Up de pipeline validado, nem plano de implementação de entrega concluída.

Comece pelo diagnóstico da seção 2, implemente o R2 dentro desses limites e pare ao final para revisão humana. Não avance para R3/Silver.
