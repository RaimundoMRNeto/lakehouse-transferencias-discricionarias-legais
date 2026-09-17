-- Valida que não existem novos órfãos na tabela ponte além dos 3 históricos conhecidos
select distinct
    pp.id_proposta
from {{ ref('siconv_programa_proposta') }} pp
left join {{ ref('siconv_proposta') }} p
    on pp.id_proposta = p.id_proposta
where p.id_proposta is null
  and pp.id_proposta not in (321453, 1427146, 296629)
