with stg as (

    select * from {{ ref('population_estimates') }}

),

-- 1. Check that all admin2_pcodes are unique (except country total)
admin2_pcode_duplicates as (

    select
        admin2_pcode as failing_key,
        'admin2_pcode_duplicate' as issue,
        count(*) as detail_count
    from stg
    where not is_country_total
      and admin2_pcode is not null
    group by admin2_pcode
    having count(*) > 1

),

-- 2. Check population equals sum of all age-sex groups (within tolerance)
population_age_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'population_age_sum_mismatch' as issue,
        abs(
            population_2025 -
            (male_under_5 + female_under_5 + male_5_17 + female_5_17 +
             male_18_60 + female_18_60 + male_over_60 + female_over_60)
        ) as detail_count
    from stg
    where abs(
        population_2025 -
        (male_under_5 + female_under_5 + male_5_17 + female_5_17 +
         male_18_60 + female_18_60 + male_over_60 + female_over_60)
    ) > 100  -- Tolerance of 100 people (allowing for rounding)

),

-- 3. Check population equals male + female
population_sex_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'population_sex_sum_mismatch' as issue,
        abs(population_2025 - (male_population + female_population)) as detail_count
    from stg
    where abs(population_2025 - (male_population + female_population)) > 100

),

-- 4. Check child_under_5 equals male_under_5 + female_under_5
child_under_5_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'child_under_5_sum_mismatch' as issue,
        abs(children_under_5 - (male_under_5 + female_under_5)) as detail_count
    from stg
    where abs(children_under_5 - (male_under_5 + female_under_5)) > 50

),

-- 5. Check percentages sum to approximately 1.0
percentages_sum_deviation as (

    select
        admin2_pcode as failing_key,
        'percentages_sum_deviation' as issue,
        abs(
            pct_male_under_5 + pct_female_under_5 +
            pct_male_5_17 + pct_female_5_17 +
            pct_male_18_60 + pct_female_18_60 +
            pct_male_over_60 + pct_female_over_60 - 1.0
        ) as detail_count
    from stg
    where abs(
        pct_male_under_5 + pct_female_under_5 +
        pct_male_5_17 + pct_female_5_17 +
        pct_male_18_60 + pct_female_18_60 +
        pct_male_over_60 + pct_female_over_60 - 1.0
    ) > 0.01  -- Allow 1% deviation due to rounding

),

-- 6. Check negative values in any population count column
negative_population_values as (

    select
        admin2_pcode as failing_key,
        'negative_population_value' as issue,
        count(*) as detail_count
    from stg
    where population_2025 < 0
       or male_under_5 < 0
       or female_under_5 < 0
       or male_5_17 < 0
       or female_5_17 < 0
       or male_18_60 < 0
       or female_18_60 < 0
       or male_over_60 < 0
       or female_over_60 < 0
    group by admin2_pcode

),

-- 7. Check percentages are between 0 and 1
invalid_percentages as (

    select
        admin2_pcode as failing_key,
        'invalid_percentage' as issue,
        count(*) as detail_count
    from stg
    where pct_male_under_5 < 0 or pct_male_under_5 > 1
       or pct_female_under_5 < 0 or pct_female_under_5 > 1
       or pct_male_5_17 < 0 or pct_male_5_17 > 1
       or pct_female_5_17 < 0 or pct_female_5_17 > 1
       or pct_male_18_60 < 0 or pct_male_18_60 > 1
       or pct_female_18_60 < 0 or pct_female_18_60 > 1
       or pct_male_over_60 < 0 or pct_male_over_60 > 1
       or pct_female_over_60 < 0 or pct_female_over_60 > 1
    group by admin2_pcode

),

-- 8. Check population is greater than every age subgroup
population_greater_than_subgroups as (

    select
        admin2_pcode as failing_key,
        'population_less_than_subgroup' as issue,
        count(*) as detail_count
    from stg
    where population_2025 < male_under_5
       or population_2025 < female_under_5
       or population_2025 < male_5_17
       or population_2025 < female_5_17
       or population_2025 < male_18_60
       or population_2025 < female_18_60
       or population_2025 < male_over_60
       or population_2025 < female_over_60
    group by admin2_pcode

),

-- 9. Check derived aggregates match source (working_age = sum of 18-60)
working_age_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'working_age_sum_mismatch' as issue,
        abs(working_age_population - (male_18_60 + female_18_60)) as detail_count
    from stg
    where abs(working_age_population - (male_18_60 + female_18_60)) > 50

),

-- 10. Check elderly sum matches
elderly_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'elderly_sum_mismatch' as issue,
        abs(elderly_population - (male_over_60 + female_over_60)) as detail_count
    from stg
    where abs(elderly_population - (male_over_60 + female_over_60)) > 50

),

-- 11. Check dependency ratio calculation is consistent
dependency_ratio_check as (

    select
        admin2_pcode as failing_key,
        'dependency_ratio_mismatch' as issue,
        abs(
            dependency_ratio -
            ((children_under_5 + children_5_17 + elderly_population) /
             nullif(working_age_population, 0))
        ) as detail_count
    from stg
    where not is_country_total
      and working_age_population > 0
      and abs(
          dependency_ratio -
          ((children_under_5 + children_5_17 + elderly_population) /
           nullif(working_age_population, 0))
      ) > 0.001

),

-- 12. Check record_type classification is accurate
invalid_record_type as (

    select
        admin2_pcode as failing_key,
        'invalid_record_type' as issue,
        count(*) as detail_count
    from stg
    where (is_country_total and record_type != 'COUNTRY_TOTAL')
       or (not is_country_total
           and admin1_pcode is not null
           and admin2_pcode is not null
           and record_type != 'ADMIN2_LEVEL')
       or (not is_country_total
           and (admin1_pcode is null or admin2_pcode is null)
           and record_type != 'INCOMPLETE_ADMIN')
    group by admin2_pcode

),

-- 13. Check working_age_ratio is between 0 and 1
invalid_working_age_ratio as (

    select
        admin2_pcode as failing_key,
        'invalid_working_age_ratio' as issue,
        count(*) as detail_count
    from stg
    where working_age_ratio < 0 or working_age_ratio > 1

),

-- 14. Check child_5_17 equals sum of male_5_17 + female_5_17
child_5_17_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'child_5_17_sum_mismatch' as issue,
        abs(children_5_17 - (male_5_17 + female_5_17)) as detail_count
    from stg
    where abs(children_5_17 - (male_5_17 + female_5_17)) > 50

),

-- 15. Check male_population equals sum of male age groups
male_population_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'male_population_sum_mismatch' as issue,
        abs(male_population - (male_under_5 + male_5_17 + male_18_60 + male_over_60)) as detail_count
    from stg
    where abs(male_population - (male_under_5 + male_5_17 + male_18_60 + male_over_60)) > 50

),

-- 16. Check female_population equals sum of female age groups
female_population_sum_mismatch as (

    select
        admin2_pcode as failing_key,
        'female_population_sum_mismatch' as issue,
        abs(female_population - (female_under_5 + female_5_17 + female_18_60 + female_over_60)) as detail_count
    from stg
    where abs(female_population - (female_under_5 + female_5_17 + female_18_60 + female_over_60)) > 50

),

-- 17. Check that admin2_pcode not null for non-country rows
admin2_null_for_non_country as (

    select
        'country_has_admin_code' as failing_key,
        'admin2_present_on_country_row' as issue,
        count(*) as detail_count
    from stg
    where is_country_total
      and (admin1_pcode is not null or admin2_pcode is not null)

),

-- 18. Check exact one country total row exists
country_total_count as (

    select
        'country_total_count' as failing_key,
        'country_total_count_mismatch' as issue,
        count(*) - 1 as detail_count  -- Expect exactly 1 country total row
    from stg
    where is_country_total = true

)

select * from admin2_pcode_duplicates
union all
select * from population_age_sum_mismatch
union all
select * from population_sex_sum_mismatch
union all
select * from child_under_5_sum_mismatch
union all
select * from percentages_sum_deviation
union all
select * from negative_population_values
union all
select * from invalid_percentages
union all
select * from population_greater_than_subgroups
union all
select * from working_age_sum_mismatch
union all
select * from elderly_sum_mismatch
union all
select * from dependency_ratio_check
union all
select * from invalid_record_type
union all
select * from invalid_working_age_ratio
union all
select * from child_5_17_sum_mismatch
union all
select * from male_population_sum_mismatch
union all
select * from female_population_sum_mismatch
union all
select * from admin2_null_for_non_country
union all
select * from country_total_count