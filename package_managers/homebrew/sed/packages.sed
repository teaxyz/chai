1d
's/"\([^"]*\)","\([^"]*\)","\([^"]*\)",*/INSERT INTO package (derived_id, import_id, name, readme) VALUES ("\1", "\2", "\3", NULL);/'