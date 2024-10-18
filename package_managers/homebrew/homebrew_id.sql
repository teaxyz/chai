WITH homebrew_source_id AS (
    INSERT INTO sources ("type")
    VALUES ('homebrew')
    ON CONFLICT ("type") DO UPDATE SET "type" = EXCLUDED."type"
    RETURNING id
), package_manager_id AS (
    INSERT INTO package_managers (source_id)
    VALUES ((SELECT id FROM homebrew_source_id))
    RETURNING id
)
SELECT id
FROM package_manager_id;