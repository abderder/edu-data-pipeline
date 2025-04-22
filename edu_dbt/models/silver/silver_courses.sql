WITH raw AS (
    SELECT
        "CourseID" AS raw_course_id,
        "Course Name" AS raw_course_name,
        "credits" AS raw_credits,
        "Start_Date" AS raw_start_date,
        "endDate" AS raw_end_date
    FROM {{ source('BRONZE', 'RAW_COURSES') }}
),

cleaned AS (
    SELECT
        CAST(raw_course_id AS INT) AS course_id,

        -- Nettoyage du nom du cours (capitalisation, suppression espaces, etc.)
        INITCAP(TRIM(raw_course_name)) AS course_name,

        CAST(raw_credits AS INT) AS credits,

        -- Formats de dates divers
        COALESCE(
            TRY_TO_DATE(raw_start_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_start_date, 'DD-MM-YYYY'),
            TRY_TO_DATE(raw_start_date, 'DD/MM/YYYY'),
            TRY_TO_DATE(raw_start_date, 'YYYY/MM/DD')
        ) AS start_date,

        COALESCE(
            TRY_TO_DATE(raw_end_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_end_date, 'DD-MM-YYYY'),
            TRY_TO_DATE(raw_end_date, 'DD/MM/YYYY'),
            TRY_TO_DATE(raw_end_date, 'YYYY/MM/DD')
        ) AS end_date

    FROM raw
    WHERE raw_course_id IS NOT NULL
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY start_date DESC) AS row_num
    FROM cleaned
)
SELECT
    course_id,
    course_name,
    credits,
    start_date,
    end_date
FROM ranked
WHERE row_num = 1 and course_name is not null