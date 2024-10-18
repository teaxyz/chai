WITH inserted_rows AS (
  INSERT INTO url_types (name)
  VALUES ('source'), ('homepage')
  ON CONFLICT (name) DO NOTHING
  RETURNING id, name
)
SELECT id, name
FROM inserted_rows
UNION ALL
SELECT id, name
FROM url_types
WHERE name IN ('source', 'homepage')
  AND name NOT IN (SELECT name FROM inserted_rows);