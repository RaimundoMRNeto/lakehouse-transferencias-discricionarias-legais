-- Validação dinâmica de contagem: Silver programa_cadastral vs Bronze programa distinct IDs
with counts as (
    select
        (select count(*) from {{ ref('siconv_programa_cadastral') }}) as silver_count,
        (select count(distinct ID_PROGRAMA) from {{ source('bronze', 'siconv_programa') }}) as bronze_distinct_count
)
select *
from counts
where silver_count != bronze_distinct_count
