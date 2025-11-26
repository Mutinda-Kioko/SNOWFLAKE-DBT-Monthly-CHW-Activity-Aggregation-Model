
{{ 
  config(
    materialized = 'incremental',
    unique_key = ['chv_id', 'report_month'],
    incremental_strategy = 'delete+insert',
    schema = 'marts'
  ) 
}}



-- ============================================
-- Source data + filters
-- ============================================
with source_data as (

    select
        activity_id,
        chv_id,
        activity_date,
        activity_type,
        household_id,
        patient_id,
        is_deleted
    from {{ source('chw', 'fct_chv_activity') }}
    --filter out null values in chv_id, activity_date and is delete.
    where
        chv_id is not null
        and activity_date is not null
        and is_deleted = false

        -- Incremental logic: re-process the last ~2 months to capture late-arriving data
        {% if is_incremental() %}
        and activity_date >= (
            select dateadd('month', -2, max(report_month))
            from {{ this }}
        )
        {% endif %}

),

-- ============================================
-- Assign reporting month (26th cutoff)
-- ============================================
with_report_month as (

    select
        *,
        {{ month_assignment('activity_date') }} as report_month
    from source_data

),

-- ============================================
-- Aggregation
-- ============================================
aggregated as (

    select
        chv_id,
        report_month,

        count(*)                                          as total_activities,

        count(distinct household_id)                      as unique_households_visited,
        count(distinct patient_id)                        as unique_patients_served,

        sum(case when activity_type = 'pregnancy_visit'   then 1 else 0 end) as pregnancy_visits,
        sum(case when activity_type = 'child_assessment'  then 1 else 0 end) as child_assessments,
        sum(case when activity_type = 'family_planning'   then 1 else 0 end) as family_planning_visits

    from with_report_month
    group by chv_id, report_month

)

select * from aggregated

{{ show_results() }}