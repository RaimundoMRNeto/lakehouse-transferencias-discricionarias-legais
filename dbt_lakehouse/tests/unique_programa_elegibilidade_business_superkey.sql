-- Valida a unicidade da superchave semântica normalizada na camada Silver de elegibilidade
-- Superchave: (id_programa, modalidade_programa, natureza_juridica_programa, uf_programa, acao_orcamentaria)
select
    id_programa,
    modalidade_programa,
    natureza_juridica_programa,
    uf_programa,
    acao_orcamentaria,
    count(*) as n_records
from {{ ref('siconv_programa_elegibilidade') }}
group by
    id_programa,
    modalidade_programa,
    natureza_juridica_programa,
    uf_programa,
    acao_orcamentaria
having count(*) > 1
