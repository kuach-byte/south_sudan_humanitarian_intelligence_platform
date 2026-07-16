{{
    config(
        materialized='view',
        schema='staging',
        alias='population_estimates',
        tags=['staging', 'population', 'reference']
    )
}}

with source as (

    select
        admin1,
        admin1_pcode,
        admin2,
        admin2_pcode,
        population_2025,
        pct_male_under_5,
        male_under_5,
        pct_female_under_5,
        female_under_5,
        pct_male_5_17,
        male_5_17,
        pct_female_5_17,
        female_5_17,
        pct_male_18_60,
        male_18_60,
        pct_female_18_60,
        female_18_60,
        pct_male_over_60,
        male_over_60,
        pct_female_over_60,
        female_over_60
    from {{ source('cleaned_data', 'population_estimates_2024_clean') }}

),

standardized as (

    select
        -- Surrogate key for entity resolution
        {{ dbt_utils.generate_surrogate_key(['admin2_pcode']) }} as population_sk,

        -- Administrative hierarchy with standardization
        nullif(trim(admin1), '') as admin1,
        upper(trim(admin1_pcode)) as admin1_pcode,
        nullif(trim(admin2), '') as admin2,
        upper(trim(admin2_pcode)) as admin2_pcode,

        -- Flag for the single country-level aggregate row
        case
            when admin1_pcode is null and admin2_pcode is null then true
            else false
        end as is_country_total,

        -- Population estimates (preserve original values)
        population_2025::numeric(18,2) as population_2025,

        -- Age-sex distribution counts (cast to numeric for precision)
        male_under_5::numeric(18,2) as male_under_5,
        female_under_5::numeric(18,2) as female_under_5,
        male_5_17::numeric(18,2) as male_5_17,
        female_5_17::numeric(18,2) as female_5_17,
        male_18_60::numeric(18,2) as male_18_60,
        female_18_60::numeric(18,2) as female_18_60,
        male_over_60::numeric(18,2) as male_over_60,
        female_over_60::numeric(18,2) as female_over_60,

        -- Percentage columns (preserve original precision)
        pct_male_under_5::numeric(8,6) as pct_male_under_5,
        pct_female_under_5::numeric(8,6) as pct_female_under_5,
        pct_male_5_17::numeric(8,6) as pct_male_5_17,
        pct_female_5_17::numeric(8,6) as pct_female_5_17,
        pct_male_18_60::numeric(8,6) as pct_male_18_60,
        pct_female_18_60::numeric(8,6) as pct_female_18_60,
        pct_male_over_60::numeric(8,6) as pct_male_over_60,
        pct_female_over_60::numeric(8,6) as pct_female_over_60,

        -- Derived aggregated demographic groups
        (male_under_5 + female_under_5)::numeric(18,2) as children_under_5,
        (male_5_17 + female_5_17)::numeric(18,2) as children_5_17,
        (male_18_60 + female_18_60)::numeric(18,2) as working_age_population,
        (male_over_60 + female_over_60)::numeric(18,2) as elderly_population,

        -- Derived sex aggregates
        (male_under_5 + male_5_17 + male_18_60 + male_over_60)::numeric(18,2) as male_population,
        (female_under_5 + female_5_17 + female_18_60 + female_over_60)::numeric(18,2) as female_population,

        -- Dependency ratios (useful for humanitarian planning)
        case
            when (male_18_60 + female_18_60) > 0
            then ((male_under_5 + female_under_5 + male_over_60 + female_over_60) / 
                  (male_18_60 + female_18_60))::numeric(8,4)
            else null
        end as dependency_ratio,

        -- Child dependency ratio (under 15 is under 5 + 5_17)
        case
            when (male_18_60 + female_18_60) > 0
            then ((male_under_5 + female_under_5 + male_5_17 + female_5_17) / 
                  (male_18_60 + female_18_60))::numeric(8,4)
            else null
        end as child_dependency_ratio,

        -- Elderly dependency ratio
        case
            when (male_18_60 + female_18_60) > 0
            then ((male_over_60 + female_over_60) / 
                  (male_18_60 + female_18_60))::numeric(8,4)
            else null
        end as elderly_dependency_ratio,

        -- Working age ratio (percentage of population that is working age)
        case
            when population_2025 > 0
            then ((male_18_60 + female_18_60) / population_2025)::numeric(8,6)
            else null
        end as working_age_ratio

    from source

),

final as (

    select
        -- Surrogate key
        population_sk,

        -- Administrative fields
        admin1,
        admin1_pcode,
        admin2,
        admin2_pcode,
        is_country_total,

        -- Population total
        population_2025,

        -- Age-sex counts
        male_under_5,
        female_under_5,
        male_5_17,
        female_5_17,
        male_18_60,
        female_18_60,
        male_over_60,
        female_over_60,

        -- Age-sex percentages
        pct_male_under_5,
        pct_female_under_5,
        pct_male_5_17,
        pct_female_5_17,
        pct_male_18_60,
        pct_female_18_60,
        pct_male_over_60,
        pct_female_over_60,

        -- Derived aggregates
        children_under_5,
        children_5_17,
        working_age_population,
        elderly_population,
        male_population,
        female_population,

        -- Derived ratios
        dependency_ratio,
        child_dependency_ratio,
        elderly_dependency_ratio,
        working_age_ratio,

        -- Data quality meta-field
        case
            when is_country_total then 'COUNTRY_TOTAL'
            when admin1_pcode is null or admin2_pcode is null then 'INCOMPLETE_ADMIN'
            else 'ADMIN2_LEVEL'
        end as record_type

    from standardized

)

select * from final