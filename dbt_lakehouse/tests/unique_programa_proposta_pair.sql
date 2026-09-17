-- Testa se o par (id_programa, id_proposta) é único na tabela associativa
select
    id_programa,
    id_proposta,
    count(*) as n_records
from {{ ref('siconv_programa_proposta') }}
group by id_programa, id_proposta
having count(*) > 1
