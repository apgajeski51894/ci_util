#Avg Age
segment1_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as segment1
    FROM
        {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
        and f.segment = '{segment1}'
    """

#Age tier
segment1_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as segment1
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment1}'
    GROUP BY 1
    ORDER BY 2 DESC"""

#Gender
segment1_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as segment1
        from {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        and f.segment = '{segment1}'
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

segment1_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as segment1
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment1}'
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

segment1_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as segment1
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment1}'
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

segment1_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as segment1
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment1}'
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

segment1_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as segment1
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment1}'
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

#Avg Age
segment2_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as segment2
    FROM
        {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
        and f.segment = '{segment2}'
    """

#Age tier
segment2_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as segment2
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment2}'
    GROUP BY 1
    ORDER BY 2 DESC"""

#Gender
segment2_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as segment2
        from {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        and f.segment = '{segment2}'
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

segment2_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as segment2
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment2}'
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

segment2_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as segment2
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment2}'
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

segment2_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as segment2
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment2}'
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

segment2_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as segment2
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment2}'
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

#Avg Age
segment3_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as segment3
    FROM
        {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
        and f.segment = '{segment3}'
    """

#Age tier
segment3_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as segment3
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment3}'
    GROUP BY 1
    ORDER BY 2 DESC"""

#Gender
segment3_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as segment3
        from {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        and f.segment = '{segment3}'
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

segment3_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as segment3
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment3}'
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

segment3_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as segment3
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment3}'
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

segment3_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as segment3
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment3}'
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

segment3_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as segment3
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment3}'
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

#Avg Age
segment4_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as segment4
    FROM
        {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
        and f.segment = '{segment4}'
    """
#Age tier
segment4_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as segment4
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment4}'
    GROUP BY 1
    ORDER BY 2 DESC"""

#Gender
segment4_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as segment4
        from {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        and f.segment = '{segment4}'
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

segment4_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as segment4
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment4}'
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

segment4_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as segment4
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment4}'
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

segment4_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as segment4
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment4}'
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

segment4_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as segment4
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment4}'
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

#Avg Age
segment5_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as segment5
    FROM
        {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
        and f.segment = '{segment5}'
    """
#Age tier
segment5_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as segment5
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment5}'
    GROUP BY 1
    ORDER BY 2 DESC"""

#Gender
segment5_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as segment5
        from {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        and f.segment = '{segment5}'
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

segment5_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as segment5
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment5}'
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

segment5_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as segment5
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment5}'
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

segment5_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as segment5
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment5}'
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

segment5_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as segment5
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment5}'
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

#Avg Age
sub_category_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as sub_category
    FROM
        {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
        and f.segment not in ('{segment1}')
    """
#Age tier
sub_category_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as sub_category
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment not in ('{segment1}')
    GROUP BY 1
    ORDER BY 2 DESC"""

#Gender
sub_category_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as sub_category
        from {segment_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        and f.segment not in ('{segment1}')
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

sub_category_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as sub_category
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment not in ('{segment1}')
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

sub_category_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as sub_category
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment not in ('{segment1}')
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

sub_category_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as sub_category
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment not in ('{segment1}')
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

sub_category_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as sub_category
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment not in ('{segment1}')
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

#Avg Age
category_avg_age_query = """
    SELECT
        ROUND(
            AVG(
                DATE_DIFF('year', customers.birth_date, CURRENT_DATE)
            )
        ) as category
    FROM
        {category_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    """
#Age tier
category_age_tier_query = """
    SELECT
        CASE
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 15) THEN '0-14'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 15) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 25) THEN '15-24'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 25) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 35) THEN '25-34'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 35) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 45) THEN '35-44'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 45) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 55) THEN '45-54'
            WHEN (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) >= 55) AND (DATE_DIFF('year',customers.birth_date,CURRENT_DATE) < 65) THEN '55-64'
            ELSE '65+'
        END
            AS "age_tier",
    COUNT(DISTINCT f.customer_id) as category
    from {category_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    GROUP BY 1
    ORDER BY 2 DESC"""

category_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as category
    from {category_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    GROUP BY 1
    ORDER BY 2 DESC"""

segment1_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as segment1
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment1}'
    GROUP BY 1
    ORDER BY 2 DESC"""

segment2_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as segment2
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment2}'
    GROUP BY 1
    ORDER BY 2 DESC"""

segment3_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as segment3
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment3}'
    GROUP BY 1
    ORDER BY 2 DESC"""

segment4_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as segment4
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment4}'
    GROUP BY 1
    ORDER BY 2 DESC"""

segment5_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as segment5
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment = '{segment5}'
    GROUP BY 1
    ORDER BY 2 DESC"""

sub_category_region_query = """
    SELECT
    CASE
    WHEN customers.state IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'AE','NJ', 'NY', 'PA', 'VI') THEN 'Northeast'
    WHEN customers.state IN ('IN', 'IL', 'MI', 'OH', 'WI','IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'Midwest'
    WHEN customers.state IN ('DE', 'DC', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'PR', 'AA','AL', 'KY', 'MS', 'TN','AR', 'LA', 'OK', 'TX') THEN 'South'
    WHEN customers.state IN ('AZ', 'CO', 'ID', 'NM', 'MT', 'UT', 'NV', 'WY','AK', 'CA', 'HI', 'OR', 'WA', 'AS', 'FM', 'GU', 'MP', 'PW', 'AP') THEN 'West'
    END
    AS "region",
    COUNT(DISTINCT f.customer_id) as sub_category
    from {segment_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    and f.segment not in ('{segment1}')
    GROUP BY 1
    ORDER BY 2 DESC"""
#Gender
category_gender_query = """
    SELECT
        customers.gender AS "gender",
        COUNT(DISTINCT f.customer_id) as category
        from {category_table} f
        inner join vw_customers AS customers ON f.customer_id = customers.id
        WHERE true
        AND customers.gender in ('F', 'M')
    GROUP BY
        1
    ORDER BY
        2 DESC"""

category_hh_income_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.household_income = '<25000' THEN '0-25k'
        WHEN customers.household_income = '25000-50000' THEN '25-50k'
        WHEN customers.household_income = '50000-75000' THEN '50-75k'
        WHEN customers.household_income = '75000-100000' THEN '75-100k'
        WHEN customers.household_income = '>100000' THEN '100k+' ELSE NULL END as hh_income,
    COUNT(DISTINCT f.customer_id) as category
    from {category_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    AND customers.household_income is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.hh_income is not null"""

category_education_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.education in ('Masters','Doctorate','Graduate School') THEN 'Advanced Degree'
        WHEN customers.education in ('Bachelor''s','Bachelor Degree','Associates','Some College (No Degree)','College','Bachelor''s degree') THEN 'College' ELSE 'No College' END as education,
    COUNT(DISTINCT f.customer_id) as category
    from {category_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    AND customers.education is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Education is not null"""

category_ethnicity_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.ethnicity in ('Caucasian','White') THEN 'Caucasian'
        WHEN customers.ethnicity in ('Hispanic or Latino') THEN 'Hispanic'
        WHEN customers.ethnicity in ('African American') THEN 'African American'
        WHEN customers.ethnicity in ('Asian/ Pacific Islander','Filipino') THEN 'Asian/ Pacific Islander'
    ELSE NULL END as ethnicity,
    COUNT(DISTINCT f.customer_id) as category
    from {category_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    AND customers.ethnicity is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.Ethnicity is not null"""

category_kids_in_hh_query = """SELECT *
    FROM
    (
    SELECT
        CASE WHEN customers.children_in_household = 0 THEN 0
        WHEN customers.children_in_household = 1 THEN 1
        WHEN customers.children_in_household = 2 THEN 2
        WHEN customers.children_in_household = 3 THEN 3
        WHEN customers.children_in_household = 4 THEN 4
    ELSE NULL END as kids_in_hh,
    COUNT(DISTINCT f.customer_id) as category
    from {category_table} f
    inner join vw_customers AS customers ON f.customer_id = customers.id
    WHERE true
    AND customers.children_in_household is not null
    GROUP BY 1
    ORDER BY 2 DESC
    ) a
    WHERE a.kids_in_hh is not null"""

hh_penn_cy_query = """
with segment_buyers as(
  select
  segment,
  cast(count(distinct customer_id) as double) as buyers
  from {segment_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 1),
hh_penn_base as(
  select
  cast(count(distinct customer_id) as double) as total_pop
  from {active_table})
SELECT
sb.segment,
(sb.buyers / hpb.total_pop) as hh_penn_cy
from segment_buyers sb, hh_penn_base hpb
group by 1,2
"""

hh_penn_py_query = """
with segment_buyers as(
  select
  segment,
  cast(count(distinct customer_id) as double) as buyers
  from {segment_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 1),
hh_penn_base as(
  select
  cast(count(distinct customer_id) as double) as total_pop
  from {active_table})
SELECT
sb.segment,
(sb.buyers / hpb.total_pop) as hh_penn_py
from segment_buyers sb, hh_penn_base hpb
group by 1,2
"""

segment_buy_rate_cy_query = """
with data as(
  SELECT
  segment,
  customer_id,
  cast(sum(price) as double) as spend
  from {segment_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 2,1)
SELECT
segment,
avg(spend) as segment_average_buy_rate_cy,
approx_percentile(spend,.5) as segment_median_buy_rate_cy
from data
group by 1
"""

segment_buy_rate_py_query = """
with data as(
  SELECT
  segment,
  customer_id,
  cast(sum(price) as double) as spend
  from {segment_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 2,1)
SELECT
segment,
avg(spend) as segment_average_buy_rate_py,
approx_percentile(spend,.5) as segment_median_buy_rate_py
from data
group by 1
"""

segment_units_cy_query = """
with data as(
  SELECT
  segment,
  customer_id,
  cast(sum(quantity) as double) as units
  from {segment_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 2,1)
SELECT
segment,
avg(units) as segment_average_units_cy,
approx_percentile(units,.5) as segment_median_units_cy
from data
group by 1
"""

segment_units_py_query = """
with data as(
  SELECT
  segment,
  customer_id,
  cast(sum(quantity) as double) as units
  from {segment_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 2,1)
SELECT
segment,
avg(units) as segment_average_units_py,
approx_percentile(units,.5) as segment_median_units_py
from data
group by 1
"""

category_buy_rate_cy_query = """
with category_spend as(
  SELECT
  customer_id,
  cast(sum(price) as double) as cat_spend
  from {category_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.cat_spend
  from {segment_table} s
  inner join category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(cat_spend) as category_average_buy_rate_cy,
approx_percentile(cat_spend,.5) as category_median_buy_rate_cy
from data
group by 1
"""

category_buy_rate_py_query = """
with category_spend as(
  SELECT
  customer_id,
  cast(sum(price) as double) as cat_spend
  from {category_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.cat_spend
  from {segment_py_table} s
  inner join category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(cat_spend) as category_average_buy_rate_py,
approx_percentile(cat_spend,.5) as category_median_buy_rate_py
from data
group by 1
"""


category_units_cy_query = """
with category_spend as(
  SELECT
  customer_id,
  cast(sum(quantity) as double) as cat_units
  from {category_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.cat_units
  from {segment_table} s
  inner join category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(cat_units) as category_average_units_cy,
approx_percentile(cat_units,.5) as category_median_units_cy
from data
group by 1
"""

category_units_py_query = """
with category_spend as(
  SELECT
  customer_id,
  cast(sum(quantity) as double) as cat_units
  from {category_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.cat_units
  from {segment_py_table} s
  inner join category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(cat_units) as category_average_units_py,
approx_percentile(cat_units,.5) as category_median_units_py
from data
group by 1
"""

sub_category_buy_rate_cy_query = """
with sub_category_spend as(
  SELECT
  customer_id,
  cast(sum(price) as double) as sub_cat_spend
  from {segment_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.sub_cat_spend
  from {segment_table} s
  inner join sub_category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(sub_cat_spend) as sub_category_average_buy_rate_cy,
approx_percentile(sub_cat_spend,.5) as sub_category_median_buy_rate_cy
from data
group by 1
"""

sub_category_buy_rate_py_query = """
with sub_category_spend as(
  SELECT
  customer_id,
  cast(sum(price) as double) as sub_cat_spend
  from {segment_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.sub_cat_spend
  from {segment_py_table} s
  inner join sub_category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(sub_cat_spend) as sub_category_average_buy_rate_py,
approx_percentile(sub_cat_spend,.5) as sub_category_median_buy_rate_py
from data
group by 1
"""


sub_category_units_cy_query = """
with sub_category_spend as(
  SELECT
  customer_id,
  cast(sum(quantity) as double) as sub_cat_units
  from {segment_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.sub_cat_units
  from {segment_table} s
  inner join sub_category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(sub_cat_units) as sub_category_average_units_cy,
approx_percentile(sub_cat_units,.5) as sub_category_median_units_cy
from data
group by 1
"""

sub_category_units_py_query = """
with sub_category_spend as(
  SELECT
  customer_id,
  cast(sum(quantity) as double) as sub_cat_units
  from {segment_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 1),
data as(
  SELECT
  s.customer_id,
  s.segment,
  cs.sub_cat_units
  from {segment_py_table} s
  inner join sub_category_spend cs on cs.customer_id = s.customer_id
  group by 1,2,3)
SELECT
segment,
avg(sub_cat_units) as sub_category_average_units_py,
approx_percentile(sub_cat_units,.5) as sub_category_median_units_py
from data
group by 1
"""

market_share_category_units_cy_query = """
with segment as(
  SELECT
  segment,
  cast(sum(quantity) as double) as segment_units
  from {segment_table}
  group by 1),
category_total as(
  select
  cast(sum(quantity) as double)as total_units
  from {category_table})
SELECT
s.segment,
(s.segment_units / c.total_units) as market_share_category_units_cy
from segment s, category_total c
group by 1,2
"""

market_share_category_units_py_query = """
with segment as(
  SELECT
  segment,
  cast(sum(quantity) as double) as segment_units
  from {segment_py_table}
  group by 1),
category_total as(
  select
  cast(sum(quantity) as double)as total_units
  from {category_py_table})
SELECT
s.segment,
(s.segment_units / c.total_units) as market_share_category_units_py
from segment s, category_total c
group by 1,2
"""

market_share_category_dollars_cy_query = """
with segment as(
  SELECT
  segment,
  cast(sum(price) as double) as segment_volume
  from {segment_table}
  group by 1),
category_total as(
  select
  cast(sum(price) as double) as total_volume
  from {category_table})
SELECT
s.segment,
(s.segment_volume / c.total_volume) as market_share_category_dollars_cy
from segment s, category_total c
group by 1,2
"""

market_share_category_dollars_py_query = """
with segment as(
  SELECT
  segment,
  cast(sum(price) as double) as segment_volume
  from {segment_py_table}
  group by 1),
category_total as(
  select
  cast(sum(price) as double) as total_volume
  from {category_py_table})
SELECT
s.segment,
(s.segment_volume / c.total_volume) as market_share_category_dollars_py
from segment s, category_total c
group by 1,2
"""

market_share_sub_category_units_cy_query = """
with segment as(
  SELECT
  segment,
  cast(sum(quantity) as double) as segment_units
  from {segment_table}
  group by 1),
category_total as(
  select
  cast(sum(quantity) as double) as total_units
  from {segment_table}
  WHERE
  segment not in('{segment1}'))
SELECT
s.segment,
(s.segment_units / c.total_units) as market_share_sub_category_units_cy
from segment s, category_total c
group by 1,2
"""

market_share_sub_category_units_py_query = """
with segment as(
  SELECT
  segment,
  cast(sum(quantity) as double) as segment_units
  from {segment_py_table}
  group by 1),
category_total as(
  select
  cast(sum(quantity) as double) as total_units
  from {segment_py_table}
  WHERE
  segment not in('{segment1}'))
SELECT
s.segment,
(s.segment_units / c.total_units) as market_share_sub_category_units_py
from segment s, category_total c
group by 1,2
"""

market_share_sub_category_dollars_cy_query = """
with segment as(
  SELECT
  segment,
  cast(sum(price) as double) as segment_volume
  from {segment_table}
  group by 1),
category_total as(
SELECT
cast(sum(price) as double) as total_volume
from {segment_table}
WHERE
segment not in('{segment1}'))
SELECT
s.segment,
(s.segment_volume / c.total_volume) as market_share_sub_category_dollars_cy
from segment s, category_total c
group by 1,2
"""

market_share_sub_category_dollars_py_query = """
with segment as(
  SELECT
  segment,
  cast(sum(price) as double) as segment_volume
  from {segment_py_table}
  group by 1),
category_total as(
  select
  cast(sum(price) as double) as total_volume
  from {segment_py_table}
  WHERE
  segment not in('{segment1}'))
SELECT
s.segment,
(s.segment_volume / c.total_volume) as market_share_sub_category_dollars_py
from segment s, category_total c
group by 1,2
"""

times_purchased_cy_query = """
with trips as(
  SELECT
  segment,
  customer_id,
  cast(count(distinct receipt_id) as double) as purchases
  from {segment_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_table})
  group by 2,1)
SELECT
segment,
avg(purchases) as avg_times_purchased_cy,
approx_percentile(purchases,.5) as median_times_purchased_cy
from trips
group by 1
"""

times_purchased_py_query = """
with trips as(
  SELECT
  segment,
  customer_id,
  cast(count(distinct receipt_id) as double) as purchases
  from {segment_py_table}
  WHERE
  customer_id in(
    SELECT
    customer_id
    from {active_py_table})
  group by 2,1)
SELECT
segment,
avg(purchases) as avg_times_purchased_py,
approx_percentile(purchases,.5) as median_times_purchased_py
from trips
group by 1
"""

segment1_trial_repeat_query = """
SELECT
distinct receipt_id,
customer_id,
receipt_created_at
from {segment_table}
where
True
and segment = '{segment1}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
order by customer_id,
         receipt_id,
         receipt_created_at ASC
"""

segment2_trial_repeat_query = """
SELECT
distinct receipt_id,
customer_id,
receipt_created_at
from {segment_table}
where
True
and segment = '{segment2}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
order by customer_id,
         receipt_id,
         receipt_created_at ASC
"""

segment3_trial_repeat_query = """
SELECT
distinct receipt_id,
customer_id,
receipt_created_at
from {segment_table}
where
True
and segment = '{segment3}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
order by customer_id,
         receipt_id,
         receipt_created_at ASC
"""

segment4_trial_repeat_query = """
SELECT
distinct receipt_id,
customer_id,
receipt_created_at
from {segment_table}
where
True
and segment = '{segment4}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
order by customer_id,
         receipt_id,
         receipt_created_at ASC
"""

segment5_trial_repeat_query = """
SELECT
distinct receipt_id,
customer_id,
receipt_created_at
from {segment_table}
where
True
and segment = '{segment5}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
order by customer_id,
         receipt_id,
         receipt_created_at ASC
"""


segment2_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as segment2_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment2}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment2_total_query = """
SELECT
count(distinct customer_id) as segment2_total
from {segment_table}
WHERE
True
and segment = '{segment2}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
"""

segment3_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as segment3_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment3}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment3_total_query = """
SELECT
count(distinct customer_id) as segment3_total
from {segment_table}
WHERE
True
and segment = '{segment3}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
"""

segment4_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as segment4_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment4}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment4_total_query = """
SELECT
count(distinct customer_id) as segment4_total
from {segment_table}
WHERE
True
and segment = '{segment4}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
"""

segment5_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as segment5_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment5}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment5_total_query = """
SELECT
count(distinct customer_id) as segment5_total
from {segment_table}
WHERE
True
and segment = '{segment5}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
"""

segment1_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as segment1_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment1}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment1_total_query = """
SELECT
count(distinct customer_id) as segment1_total
from {segment_table}
WHERE
True
and segment = '{segment1}'
and customer_id in(
  SELECT
  customer_id
  from {active_table})
"""

category_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as category_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {category_table})
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

category_total_query = """
SELECT
count(distinct customer_id) as category_total
from {category_table}
where
customer_id in(
  SELECT
  customer_id
  from {active_table})
"""

sub_category_top_brands_query = """
SELECT
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct f.customer_id) as double) as sub_category_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_brands b on f.brand_id = b.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  segment not in ('{segment1}'))
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

sub_category_total_query = """
SELECT
count(distinct customer_id) as sub_category_total
from {segment_table}
where
customer_id in(
  SELECT
  customer_id
  from {active_table})
and segment not in ('{segment1}')
"""

segment1_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as segment1_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment1}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment2_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as segment2_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment2}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment3_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as segment3_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment3}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment4_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as segment4_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment4}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

segment5_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as segment5_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment = '{segment5}')
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

category_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as category_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {category_table})
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

sub_category_top_categories_query = """
SELECT
pc.name as category_name,
cast(count(distinct f.customer_id) as double) as sub_category_purchasers
from vw_fact_customer_receipt_item_product_details f
inner join vw_product_categories pc on f.tertiary_category_id = pc.id
WHERE
f.receipt_created_at >= date('{start_date}')
and f.receipt_created_at < date('{end_date}')
and f.funded_offer_id is NULL
and f.customer_id in(
  SELECT
  customer_id
  from {segment_table}
  WHERE
  True
  and segment not in ('{segment1}'))
and f.customer_id in(
  SELECT
  customer_id
  from {active_table})
group by 1
"""

category_top_competitive_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct c.customer_id) as double) as category_purchasers
from {category_table} c
inner join vw_brands b on c.brand_id = b.id
where
customer_id in(
  SELECT
  customer_id
  from {active_table})
and receipt_created_at >= date('{start_date}')
and receipt_created_at < date('{end_date}')
group by 1
"""

sub_category_top_competitive_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as sub_category_purchasers
from {segment_table} s
inner join vw_brands b on s.brand_id = b.id
where
s.segment not in ('{segment1}')
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and s.receipt_created_at >= date('{start_date}')
and s.receipt_created_at < date('{end_date}')
group by 1
"""

segment1_top_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment1_purchasers
from {category_table} c
inner join vw_brands b on c.brand_id = b.id
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment1}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment2_top_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment2_purchasers
from {category_table} c
inner join vw_brands b on c.brand_id = b.id
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment2}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment3_top_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment3_purchasers
from {category_table} c
inner join vw_brands b on c.brand_id = b.id
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment3}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment4_top_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment4_purchasers
from {category_table} c
inner join vw_brands b on c.brand_id = b.id
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment4}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment5_top_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment5_purchasers
from {category_table} c
inner join vw_brands b on c.brand_id = b.id
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment5}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment1_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment1_purchasers
from {segment_table} s
inner join vw_brands b on s.brand_id = b.id
where
s.segment = '{segment1}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and s.receipt_created_at >= date('{start_date}')
and s.receipt_created_at < date('{end_date}')
group by 1
"""

segment2_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment2_purchasers
from {segment_table} s
inner join vw_brands b on s.brand_id = b.id
where
s.segment = '{segment2}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and s.receipt_created_at >= date('{start_date}')
and s.receipt_created_at < date('{end_date}')
group by 1
"""

segment3_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment3_purchasers
from {segment_table} s
inner join vw_brands b on s.brand_id = b.id
where
s.segment = '{segment3}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and s.receipt_created_at >= date('{start_date}')
and s.receipt_created_at < date('{end_date}')
group by 1
"""

segment4_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment4_purchasers
from {segment_table} s
inner join vw_brands b on s.brand_id = b.id
where
s.segment = '{segment4}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and s.receipt_created_at >= date('{start_date}')
and s.receipt_created_at < date('{end_date}')
group by 1
"""

segment5_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct s.customer_id) as double) as segment5_purchasers
from {segment_table} s
inner join vw_brands b on s.brand_id = b.id
where
s.segment = '{segment5}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and s.receipt_created_at >= date('{start_date}')
and s.receipt_created_at < date('{end_date}')
group by 1
"""

category_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct c.customer_id) as double) as category_purchasers
from {category_table} c
inner join (select brand_name from {segment_table}) s on c.brand_name = s.brand_name
inner join vw_brands b on s.brand_id = b.id
where
c.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

sub_category_top_sub_category_brands_query = """
select
case when b.private_label = 1 then 'Store Brand'
  else b.name
  end as brand_name,
cast(count(distinct c.customer_id) as double) as sub_category_purchasers
from (select customer_id, brand_name, price, receipt_created_at from {segment_table}) c
inner join vw_brands b on c.brand_id = b.id
where
c.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

category_top_competitive_products_query = """
select
global_product_name,
cast(count(distinct customer_id) as double) as category_purchasers
from {category_table}
where
customer_id in(
  SELECT
  customer_id
  from {active_table})
and receipt_created_at >= date('{start_date}')
and receipt_created_at < date('{end_date}')
group by 1
"""

segment1_top_competitive_products_query = """
select
c.global_product_name,
cast(count(distinct s.customer_id) as double) as segment1_purchasers
from {category_table} c
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment1}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment2_top_competitive_products_query = """
select
c.global_product_name,
cast(count(distinct s.customer_id) as double) as segment2_purchasers
from {category_table} c
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment2}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment3_top_competitive_products_query = """
select
c.global_product_name,
cast(count(distinct s.customer_id) as double) as segment3_purchasers
from {category_table} c
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment3}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment4_top_competitive_products_query = """
select
c.global_product_name,
cast(count(distinct s.customer_id) as double) as segment4_purchasers
from {category_table} c
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment4}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

segment5_top_competitive_products_query = """
select
c.global_product_name,
cast(count(distinct s.customer_id) as double) as segment4_purchasers
from {category_table} c
inner join {segment_table} s on s.customer_id = c.customer_id
where
s.segment = '{segment5}'
and s.customer_id in(
  SELECT
  customer_id
  from {active_table})
and c.receipt_created_at >= date('{start_date}')
and c.receipt_created_at < date('{end_date}')
group by 1
"""

total_trip_basket_metrics = """
with segment as(
SELECT
receipt_id,
segment
from {segment_table}),
total_basket_data as(
SELECT
s.segment,
s.receipt_id,
cast(sum(case when
(ri.ext_price is null or ri.quantity > 1) then ri.price
else ri.ext_price end) as double) as total_spend,
cast(sum(case when (ri.quantity is not null and ri.quantity > 1) then ri.quantity
else 1 end) as double) as total_units
from vw_fact_customer_receipt_item_product_details c
inner join (select id, ext_price, price, quantity
			from vw_receipt_items) ri on c.receipt_item_id = ri.id
inner join segment s on c.receipt_id = s.receipt_id
WHERE
(((ri.ext_price is null or ri.quantity > 1) and ri.price > 0 and ri.price < 100) or ((ri.ext_price is not null and (ri.quantity = 1 or ri.quantity is null)) and ri.ext_price > 0 and ri.ext_price < 100))
group by 1,2)
SELECT
t.segment,
cast(avg(t.total_spend) as double) as average_trip_spend,
cast(approx_percentile(t.total_spend,.5) as double) as median_trip_spend,
cast(avg(t.total_units) as double) as average_trip_units,
cast(approx_percentile(t.total_units,.5) as double) as median_trip_units,
cast(avg(t.total_spend) as double)/cast(avg(t.total_units) as double) as average_trip_price_per_item,
cast(approx_percentile(t.total_spend,.5) as double)/cast(approx_percentile(t.total_units,.5) as double) as median_trip_price_per_item
from total_basket_data t
group by 1
"""

category_basket_metrics = """
with segment as(
SELECT
receipt_id,
segment
from {segment_table}),
cat_basket_data as(
SELECT
s.segment,
c.receipt_id,
cast(sum(c.price) as double) as cat_spend,
cast(sum(c.quantity) as double) as cat_units
from {category_table} c
inner join segment s on c.receipt_id = s.receipt_id
group by 1,2)
SELECT
c.segment,
cast(avg(c.cat_spend) as double) as average_category_spend,
cast(approx_percentile(c.cat_spend,.5) as double) as median_category_spend,
cast(avg(c.cat_units) as double) as average_category_units,
cast(approx_percentile(c.cat_units,.5) as double) as median_category_units,
cast(avg(c.cat_spend) as double)/cast(avg(c.cat_units) as double) as average_category_price_per_item,
cast(approx_percentile(c.cat_spend,.5) as double)/cast(approx_percentile(c.cat_units,.5) as double) as median_category_price_per_item
from cat_basket_data c
group by 1
"""

sub_category_basket_metrics = """
with segment as(
SELECT
receipt_id,
segment
from {segment_table}),
sub_cat_basket_data as(
SELECT
s.segment,
c.receipt_id,
cast(sum(c.price) as double) as sub_cat_spend,
cast(sum(c.quantity) as double) as sub_cat_units
from {segment_table} c
inner join segment s on c.receipt_id = s.receipt_id
group by 1,2)
SELECT
sc.segment,
cast(avg(sc.sub_cat_spend) as double) as average_sub_category_spend,
cast(approx_percentile(sc.sub_cat_spend,.5) as double) as median_sub_category_spend,
cast(avg(sc.sub_cat_units) as double) as average_sub_category_units,
cast(approx_percentile(sc.sub_cat_units,.5) as double) as median_sub_category_units,
cast(avg(sc.sub_cat_spend) as double)/cast(avg(sc.sub_cat_units) as double) as average_sub_category_price_per_item,
cast(approx_percentile(sc.sub_cat_spend,.5) as double)/cast(approx_percentile(sc.sub_cat_units,.5) as double) as median_sub_category_price_per_item
from sub_cat_basket_data sc
group by 1
"""
new_lapsed_retained = """
with active as(
  SELECT
  customer_id,
  segment,
  receipt_created_at
  from (
	select customer_id, receipt_created_at,segment from {segment_table}
	union
	select customer_id, receipt_created_at,segment from {segment_py_table})
  WHERE
  true
  and customer_id in(
    SELECT
    customer_id
    from {active_table})
  and customer_id in(
    SELECT
    customer_id
    from {active_py_table})),
min_product as(
SELECT
customer_id,
segment,
min(receipt_created_at) as first_purchase,
max(receipt_created_at) as last_purchase
from active a
group by 2,1),
customers as(
SELECT
segment,
cast(count(distinct case when first_purchase >= date('{start_date}') then customer_id else 0 end) as double) as new_buyers,
cast(count(distinct case when first_purchase < date('{start_date}')  and last_purchase >= date('{start_date}') then customer_id else 0 end) as double) as previous_buyers,
cast(count(distinct case when first_purchase < date('{start_date}')  and last_purchase < date('{start_date}') then customer_id else 0 end) as double) as lapsed_buyers,
cast(count(distinct customer_id) as double) as total_buyers
from min_product
group by 1)
SELECT
segment,
new_buyers,
previous_buyers,
lapsed_buyers,
(new_buyers / total_buyers) as pct_new_buyers,
(previous_buyers / total_buyers) as pct_previous_buyers,
(lapsed_buyers / total_buyers) as pct_lapsed_buyers
from customers
"""
