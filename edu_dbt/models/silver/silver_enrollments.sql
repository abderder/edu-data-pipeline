WITH raw AS (

    SELECT
        "EnrollmentId" AS raw_enrollment_id,
        "student_id" AS raw_student_id,
        "Course ID" AS raw_course_id,
        "EnrollDate" AS raw_enroll_date,
        "Status" AS raw_status
    FROM {{ source('BRONZE', 'RAW_ENROLLMENTS') }}

), cleaned AS (

    SELECT
        raw_enrollment_id AS enrollment_id,
        raw_student_id AS student_id,
        CAST(raw_course_id AS INT) AS course_id,

        COALESCE(
            TRY_TO_DATE(raw_enroll_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_enroll_date, 'DD-MM-YYYY'),
            TRY_TO_DATE(raw_enroll_date, 'YYYY/MM/DD'),
            TRY_TO_DATE(raw_enroll_date, 'DD/MM/YYYY')
        ) AS enrollment_date,

        -- Mapping plus précis des statuts
        CASE
            WHEN LOWER(raw_status) IN ('active') THEN 'active'
            WHEN LOWER(raw_status) IN ('done', 'valide', 'ok', 'completed') THEN 'completed'
            WHEN LOWER(raw_status) IN ('cancelled', 'abandon', 'dropped', 'droped') THEN 'cancelled'
            WHEN raw_status IS NULL OR TRIM(raw_status) = '' THEN 'unknown'
            ELSE 'unknown'
        END AS status

    FROM raw
    WHERE raw_enrollment_id IS NOT NULL
)

SELECT *, ROW_NUMBER() OVER (ORDER BY enrollment_id) AS enrollment_key FROM cleaned
WHERE enrollment_id IS NOT NULL AND student_id IS NOT NULL AND course_id IS NOT NULL

