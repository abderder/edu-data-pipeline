WITH raw AS (
    SELECT
        "Contact ID" AS raw_contact_id,
        "First Name" AS raw_first_name,
        "LastName" AS raw_last_name,
        "phoneNumber" AS raw_phone,
        "E-Mail" AS email_1,
        "email" AS email_2,
        "courriel" AS email_3,
        "Mail" AS email_4,
        "created_at" AS raw_created_at
    FROM EDU_DB.BRONZE.RAW_CONTACTS
),

cleaned AS (
    SELECT
        raw_contact_id AS contact_id,

        INITCAP(TRIM(raw_first_name)) AS first_name,
        INITCAP(TRIM(raw_last_name)) AS last_name,

        TRIM(raw_phone) AS phone,

        -- Priorité aux premiers emails non vides
        COALESCE(
            NULLIF(TRIM(LOWER(email_1)), ''),
            NULLIF(TRIM(LOWER(email_2)), ''),
            NULLIF(TRIM(LOWER(email_3)), ''),
            NULLIF(TRIM(LOWER(email_4)), '')
        ) AS email,

        COALESCE(
            TRY_TO_DATE(raw_created_at, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_created_at, 'DD-MM-YYYY'),
            TRY_TO_DATE(raw_created_at, 'YYYY/MM/DD'),
            TRY_TO_DATE(raw_created_at, 'DD/MM/YYYY')
        ) AS created_at

    FROM raw
    WHERE raw_contact_id IS NOT NULL
)

SELECT * FROM cleaned
where contact_id IS NOT NULL AND email IS NOT NULL