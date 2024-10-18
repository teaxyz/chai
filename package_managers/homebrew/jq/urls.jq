# homepage is at the main key
# source is inside stable, and it's the tarball
[.[] | {
  homepage: .homepage,
  source: .urls.stable.url
} | to_entries | map({
  name: .key,
  url: .value
}) | .[] | {
  url: .url,
  url_type: .name
}]