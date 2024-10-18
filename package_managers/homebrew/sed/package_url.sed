sed -E '
  1d;
  s/^"?([^,"]*),"?([^,"]*)"?$/INSERT INTO package_url (package_id, url_id) SELECT (SELECT id FROM package WHERE name = '\''\\1'\''), (SELECT id FROM url WHERE url = '\''\\2'\'');/g
' package_url.csv > package_url_inserts.sql