WITH raw AS (

    SELECT
        "Student ID"     AS raw_student_id,
        "FirstName"      AS raw_first_name,
        "last_name"      AS raw_last_name,
        "DOB"            AS raw_dob,
        "reg_date"       AS raw_reg_date,
        "email"          AS email_1,
        "E-Mail"         AS email_2,
        "mail"           AS email_3,
        "courriel"       AS email_4

    FROM {{ source('BRONZE', 'RAW_STUDENTS') }}

), cleaned AS (

    SELECT
        raw_student_id AS student_id,

        INITCAP(TRIM(raw_first_name)) AS first_name,
        INITCAP(TRIM(raw_last_name))  AS last_name,

        -- Normalisation de la date de naissance
        COALESCE(
            TRY_TO_DATE(raw_dob, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_dob, 'DD/MM/YYYY'),
            TRY_TO_DATE(raw_dob, 'DD-MM-YYYY')
        ) AS date_of_birth,

        -- Normalisation de la date d'inscription
        COALESCE(
            TRY_TO_DATE(raw_reg_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_reg_date, 'DD/MM/YYYY'),
            TRY_TO_DATE(raw_reg_date, 'DD-MM-YYYY')
        ) AS registration_date,

        -- Choix du premier email disponible (priorité dans l'ordre)
        COALESCE(NULLIF(TRIM(LOWER(email_1)), ''),
                 NULLIF(TRIM(LOWER(email_2)), ''),
                 NULLIF(TRIM(LOWER(email_3)), ''),
                 NULLIF(TRIM(LOWER(email_4)), '')
        ) AS email

    FROM raw

)

SELECT * FROM cleaned
WHERE student_id IS NOT NULL
AND email IS NOT NULL
  

