-- Validação dinâmica de contagem: Silver programa_proposta vs Bronze programa_proposta
with counts as (
    select
        (select count(*) from {{ ref('siconv_programa_proposta') }}) as silver_count,
        (select count(*) from {{ source('bronze', 'siconv_programa_proposta') }}) as bronze_count
)
select *
from counts
where silver_count != bronze_count
