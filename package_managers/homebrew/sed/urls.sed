

1d
s/"\([^"]*\)","\([^"]*\)",*/INSERT INTO urls (url, url_type_id) VALUES ('\1', (SELECT id FROM url_types WHERE "name" = '\2')) ON CONFLICT ("url", "url_type_id") DO NOTHING;/