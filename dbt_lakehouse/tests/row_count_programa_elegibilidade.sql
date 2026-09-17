-- Validação dinâmica de contagem: Silver programa_elegibilidade vs Bronze programa
with counts as (
    select
        (select count(*) from {{ ref('siconv_programa_elegibilidade') }}) as silver_count,
        (select count(*) from {{ source('bronze', 'siconv_programa') }}) as bronze_count
)
select *
from counts
where silver_count != bronze_count
