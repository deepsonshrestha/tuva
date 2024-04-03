{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      patient_id
    , encounter_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__procedure') }}