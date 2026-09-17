-- Validação dinâmica de contagem: Silver convenio vs Bronze convenio
with counts as (
    select
        (select count(*) from {{ ref('siconv_convenio') }}) as silver_count,
        (select count(*) from {{ source('bronze', 'siconv_convenio') }}) as bronze_count
)
select *
from counts
where silver_count != bronze_count
