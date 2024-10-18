1d
s/"\([^"]*\)","\([^"]*\)","\([^"]*\)",*/INSERT INTO packages (derived_id, import_id, name, package_manager_id) VALUES ('\1', '\2', '\3', '@@HOMEBREW_ID@@') ON CONFLICT ("derived_id") DO NOTHING;/