WITH raw AS (
    SELECT
        "InteractionId" AS raw_interaction_id,
        "ContactId" AS raw_contact_id,
        "Interaction Type" AS raw_type,
        "Date" AS raw_date,
        "Notes" AS raw_notes
    FROM EDU_DB.BRONZE.RAW_INTERACTIONS
),

cleaned AS (
    SELECT
        raw_interaction_id AS interaction_id,
        raw_contact_id AS contact_id,
        raw_notes,

        -- Nettoyage du type d’interaction
        CASE
            WHEN LOWER(raw_type) IN ('call', 'appel') THEN 'call'
            WHEN LOWER(raw_type) IN ('meeting') THEN 'meeting'
            WHEN LOWER(raw_type) IN ('rdv') THEN 'appointment'
            WHEN LOWER(raw_type) IN ('email') THEN 'email'
            ELSE 'unknown'
        END AS interaction_type,

        -- Gestion des dates multi-format
        COALESCE(
            TRY_TO_DATE(raw_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_date, 'DD-MM-YYYY'),
            TRY_TO_DATE(raw_date, 'DD/MM/YYYY'),
            TRY_TO_DATE(raw_date, 'YYYY/MM/DD')
        ) AS interaction_date

    FROM raw
    WHERE raw_interaction_id IS NOT NULL
)

SELECT * FROM cleaned
where interaction_id IS NOT NULL AND contact_id IS NOT NULL