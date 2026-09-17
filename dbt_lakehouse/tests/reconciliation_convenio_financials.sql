-- Reconciliação financeira dinâmica com precisão DECIMAL(38,2): Bronze vs Silver convenio
with sums as (
    select
        (select sum(cast(replace(VL_GLOBAL_CONV, ',', '.') as decimal(38,2))) from {{ source('bronze', 'siconv_convenio') }}) as bronze_vl_global,
        (select sum(cast(valor_global_convenio as decimal(38,2))) from {{ ref('siconv_convenio') }}) as silver_vl_global,
        (select sum(cast(replace(VL_REPASSE_CONV, ',', '.') as decimal(38,2))) from {{ source('bronze', 'siconv_convenio') }}) as bronze_vl_repasse,
        (select sum(cast(valor_repasse_convenio as decimal(38,2))) from {{ ref('siconv_convenio') }}) as silver_vl_repasse,
        (select sum(cast(replace(VL_CONTRAPARTIDA_CONV, ',', '.') as decimal(38,2))) from {{ source('bronze', 'siconv_convenio') }}) as bronze_vl_contrapartida,
        (select sum(cast(valor_contrapartida_convenio as decimal(38,2))) from {{ ref('siconv_convenio') }}) as silver_vl_contrapartida
)
select *
from sums
where bronze_vl_global != silver_vl_global
   or bronze_vl_repasse != silver_vl_repasse
   or bronze_vl_contrapartida != silver_vl_contrapartida
