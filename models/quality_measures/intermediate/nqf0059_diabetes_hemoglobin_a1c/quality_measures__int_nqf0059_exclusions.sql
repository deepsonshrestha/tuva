{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with advanced_illness as (

  {{ shared_exclusions__exclude_advanced_illness(
      builtins.ref('quality_measures__int_nqf0059_denominator')
      , concept_names = 
        "(
            'advanced illness'
          , 'acute inpatient'
          , 'encounter inpatient'
          , 'outpatient'
          , 'observation'
          , 'emergency department visit'
          , 'nonacute inpatient'
        )"
  )}}

)

, dementia as (

  {{ shared_exclusions__exclude_dementia(
      builtins.ref('quality_measures__int_nqf0059_denominator')
      , concept_names = 
        "(
          'dementia medications'
        )"
  )}}

)

, hospice_palliative as (

  {{ shared_exclusions__hospice_palliative(
      builtins.ref('quality_measures__int_nqf0059_denominator')
      , concept_names = 
        "(
          'hospice encounter'
        , 'palliative care encounter'
        , 'hospice care ambulatory'
        , 'hospice diagnosis'
        , 'palliative care diagnosis'
        )"
  )}}
)

, instutional_snp as (

  {{ shared_exclusions__institutional_snp(
      builtins.ref('quality_measures__int_nqf0059_denominator')
      , place_of_service_codes = "('32', '33', '34', '54', '56')"
  )}}
)

, exclusions as (

  select *
  from advanced_illness

  union all

  select *
  from dementia

  union all

  select * from hospice_palliative

  union all

  select *
  from instutional_snp

)

, valid_exclusions as (

  select 
    exclusions.*
  from exclusions
  inner join {{ref('quality_measures__int_nqf0059_denominator')}} p
      on exclusions.patient_id = p.patient_id

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , tuva_last_run
        , 1 as exclusion_flag
    from valid_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , tuva_last_run
from add_data_types
