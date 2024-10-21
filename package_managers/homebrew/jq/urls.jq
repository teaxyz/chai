# from our sources.json, we're extracting homepage and source:
  # homepage is at the main key
  # source is inside stable, and it's the tarball

# for every single row, extract the homepage and source:
[.[] | {
  homepage: .homepage,
  source: .urls.stable.url
} | to_entries | map({
# `map` basically explodes the json, creating two rows for each JSON object
  name: .key,
  url: .value
}) | .[] | 
# and here, we can generate our SQL statement!
  "INSERT INTO urls (url, url_type_id) VALUES ('" +
  .url + "', '" +
  if .name == "source" then $source_url_type_id else $homepage_url_type_id end + "')
    ON CONFLICT DO NOTHING;"
] | join("\n")
