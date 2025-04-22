{{ config(materialized='view') }}

SELECT *,ROW_NUMBER() OVER (ORDER BY COURSE_ID) AS COURSE_KEY
FROM {{ ref('silver_courses') }}
