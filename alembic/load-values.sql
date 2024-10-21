-- url types
INSERT INTO "url_types" ("name")
VALUES ('source'), ('homepage'), ('documentation'), ('repository')
ON CONFLICT (name) DO NOTHING;

-- dependency types 
INSERT INTO "depends_on_types" ("name")
VALUES
('build'),
('development'),
('runtime'),
('test'),
('optional'),
('recommended'),
('uses_from_macos')
ON CONFLICT (name) DO NOTHING;

-- sources
INSERT INTO "sources" ("type")
VALUES ('crates'), ('npm'), ('pypi'), ('rubygems'), ('github'), ('homebrew')
ON CONFLICT (type) DO NOTHING;

INSERT INTO "package_managers" ("source_id")
SELECT id
FROM "sources"
WHERE "type" IN ('crates', 'npm', 'pypi', 'rubygems', 'github', 'homebrew');
