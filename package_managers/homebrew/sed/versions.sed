1d
s%"\([^"]*\)","\([^"]*\)","\([^"]*\)",*%INSERT INTO versions (import_id, package_id, "version") VALUES ('\1', (SELECT id FROM packages WHERE derived_id = 'homebrew/\1'), '\3') ON CONFLICT ("package_id", "version") DO NOTHING;%
