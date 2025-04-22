

SELECT
    e.enrollment_key,
    s.student_key,
    s.first_name AS student_first_name,
    s.last_name AS student_last_name,
    s.email AS student_email,

    c.course_key,
    c.course_name,
    c.credits,

    e.enrollment_date,
    e.status

FROM EDU_DB.SILVER.silver_enrollments e

LEFT JOIN EDU_DB.GOLD.dim_students s
    ON e.student_id = s.student_id

LEFT JOIN EDU_DB.GOLD.dim_courses c
    ON e.course_id = c.course_id