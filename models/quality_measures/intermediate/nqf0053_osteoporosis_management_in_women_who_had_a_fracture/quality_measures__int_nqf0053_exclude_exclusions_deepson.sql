{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_nqf0053__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_nqf0053__performance_period') }}

)
{%- endset -%}

{%- set lookback_period_december -%}
(
  select 
    lookback_period_december
  from {{ ref('quality_measures__int_nqf0053__performance_period') }}

)
{%- endset -%}

with denominator as (

    select * from {{ ref('quality_measures__int_nqf0053_denominator')}}
 
)

, frailty_within_defined_window as (

  select
    *
  from {{ ref('quality_measures__shared_exclusions_frailty') }}
  where exclusion_date between 
    {{ dbt.dateadd (
        datepart = "month"
        , interval = -6
        , from_date_or_timestamp = performance_period_begin
    ) }}
    and {{ lookback_period_december }}

)


, frailty_patients_within_defined_window as (

    select
          frailty_within_defined_window.patient_id
        , frailty_within_defined_window.exclusion_date
        , frailty_within_defined_window.exclusion_reason
        , 'measure specific exclusion for defined window' as exclusion_type
    from frailty_within_defined_window
    inner join denominator
        on frailty_within_defined_window.patient_id = denominator.patient_id
    where denominator.age >= 81
    


)
select * from frailty_patients_within_defined_window
