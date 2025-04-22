WITH raw AS (
    SELECT
        "LeadID" AS raw_lead_id,
        "Full Name" AS raw_full_name,
        "Source" AS raw_source,
        "Status" AS raw_status,
        "CreatedDate" AS raw_created_date
    FROM EDU_DB.BRONZE.RAW_LEADS
),

cleaned AS (
    SELECT
        raw_lead_id AS lead_id,

        INITCAP(TRIM(raw_full_name)) AS full_name,

        -- Standardisation de la source
        CASE
            WHEN LOWER(raw_source) IN ('social', 'social media') THEN 'social'
            WHEN LOWER(raw_source) IN ('web') THEN 'web'
            WHEN LOWER(raw_source) IN ('email', 'e-mail') THEN 'email'
            WHEN LOWER(raw_source) IN ('referral', 'referal') THEN 'referral'
            ELSE 'unknown'
        END AS source,

        -- Standardisation du statut
        CASE
            WHEN LOWER(raw_status) = 'new' THEN 'new'
            WHEN LOWER(raw_status) = 'qualified' THEN 'qualified'
            WHEN LOWER(raw_status) = 'contacted' THEN 'contacted'
            WHEN LOWER(raw_status) = 'lost' THEN 'lost'
            ELSE 'unknown'
        END AS status,

        -- Parsing de la date
        COALESCE(
            TRY_TO_DATE(raw_created_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_created_date, 'DD-MM-YYYY'),
            TRY_TO_DATE(raw_created_date, 'YYYY/MM/DD'),
            TRY_TO_DATE(raw_created_date, 'DD/MM/YYYY')
        ) AS created_date

    FROM raw
    WHERE raw_lead_id IS NOT NULL
)

SELECT * FROM cleaned
where lead_id IS NOT NULL AND status IS NOT NULL AND source IS NOT NULL