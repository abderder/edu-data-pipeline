{{ config(materialized='view') }}

SELECT *, ROW_NUMBER() OVER (ORDER BY STUDENT_ID) AS student_key

FROM {{ ref('silver_students') }}
