1d
s%"\([^"]*\)","\([^"]*\)",*%INSERT INTO package_urls (package_id, url_id) SELECT (SELECT id FROM packages WHERE derived_id = 'homebrew/\1'), (SELECT id FROM urls WHERE url = '\2') ON CONFLICT ("package_id", "url_id") DO NOTHING;%
