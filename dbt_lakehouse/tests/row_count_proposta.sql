-- Validação dinâmica de contagem: Silver proposta vs Bronze proposta
with counts as (
    select
        (select count(*) from {{ ref('siconv_proposta') }}) as silver_count,
        (select count(*) from {{ source('bronze', 'siconv_proposta') }}) as bronze_count
)
select *
from counts
where silver_count != bronze_count
