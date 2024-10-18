WITH source_id AS (
    INSERT INTO sources ("type")
    VALUES ('homebrew')
    ON CONFLICT ("type") DO UPDATE SET "type" = EXCLUDED."type"
    RETURNING id
)
SELECT id
FROM package_managers
WHERE source_id = (SELECT id FROM source_id);