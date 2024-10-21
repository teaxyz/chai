SELECT
    pm.id AS package_manager_id,
    (SELECT id FROM url_types WHERE name = 'homepage') AS homepage_url_type_id,
    (SELECT id FROM url_types WHERE name = 'source') AS source_url_type_id,
    (
        SELECT id FROM depends_on_types WHERE name = 'build'
    ) AS build_depends_on_type_id,
    (
        SELECT id FROM depends_on_types WHERE name = 'runtime'
    ) AS runtime_depends_on_type_id,
    (
        SELECT id FROM depends_on_types WHERE name = 'recommended'
    ) AS recommended_depends_on_type_id,
    (
        SELECT id FROM depends_on_types WHERE name = 'optional'
    ) AS optional_depends_on_type_id,
    (
        SELECT id FROM depends_on_types WHERE name = 'test'
    ) AS test_depends_on_type_id
FROM
    package_managers AS pm
INNER JOIN
    sources AS s ON pm.source_id = s.id
WHERE
    s.type = 'homebrew';
