1d
s%"\([^"]*\)","\([^"]*\)",*%INSERT INTO dependencies (version_id, dependency_id, dependency_type_id) SELECT (SELECT id FROM versions WHERE import_id = '\3'), (SELECT id FROM packages WHERE derived_id = 'homebrew/\1'), (SELECT id FROM dependency_types WHERE name = '\2');%
