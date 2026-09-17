-- Reconciliação financeira dinâmica com precisão DECIMAL(38,2): Bronze vs Silver proposta
with sums as (
    select
        (select sum(cast(replace(VL_GLOBAL_PROP, ',', '.') as decimal(38,2))) from {{ source('bronze', 'siconv_proposta') }}) as bronze_vl_global,
        (select sum(cast(valor_global_proposta as decimal(38,2))) from {{ ref('siconv_proposta') }}) as silver_vl_global,
        (select sum(cast(replace(VL_REPASSE_PROP, ',', '.') as decimal(38,2))) from {{ source('bronze', 'siconv_proposta') }}) as bronze_vl_repasse,
        (select sum(cast(valor_repasse_proposta as decimal(38,2))) from {{ ref('siconv_proposta') }}) as silver_vl_repasse,
        (select sum(cast(replace(VL_CONTRAPARTIDA_PROP, ',', '.') as decimal(38,2))) from {{ source('bronze', 'siconv_proposta') }}) as bronze_vl_contrapartida,
        (select sum(cast(valor_contrapartida_proposta as decimal(38,2))) from {{ ref('siconv_proposta') }}) as silver_vl_contrapartida
)
select *
from sums
where bronze_vl_global != silver_vl_global
   or bronze_vl_repasse != silver_vl_repasse
   or bronze_vl_contrapartida != silver_vl_contrapartida
