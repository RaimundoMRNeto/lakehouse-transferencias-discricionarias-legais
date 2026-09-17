-- Valida a consistência lógica dos campos de conflito de convênio
select
    id_convenio_observacao,
    numero_convenio,
    source_conflict_count,
    has_source_conflict
from {{ ref('siconv_convenio') }}
where (source_conflict_count > 1 and not has_source_conflict)
   or (source_conflict_count <= 1 and has_source_conflict)
