# mapping package to urls is straightforward
# but, in the first normal form we've gotta do the mapping ourselves
# luckily, homebrew is small enough that we can push some of that work to the db

[.[] | {
  package_name: .name,
  homepage_url: .homepage,
  source_url: .urls.stable.url
} | 
  # here's where we substitute the url type ids, for each url type
    {package_name: .package_name, type: $homepage_url_type_id, url: .homepage_url},
    {package_name: .package_name, type: $source_url_type_id, url: .source_url}
  |
  # and here we say "for each url, generate an insert statement"
  "INSERT INTO package_urls (package_id, url_id) VALUES (
    (SELECT id FROM packages WHERE import_id = '" + .package_name + "'),
    (SELECT id FROM urls WHERE url = '" + .url + "' AND url_type_id = '" + .type + "'))
    ON CONFLICT DO NOTHING;"
] | join("\n")
