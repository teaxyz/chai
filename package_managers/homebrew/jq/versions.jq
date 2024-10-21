# homebrew has the problem where there are no versions
# we're gonna assume the version available is the latest

# TODO: `downloads: .analytics.install_on_request."365d".[$name]`
# above gives us the downloads for the last 365 days
# not available in the full JSON API

# TODO: there are also a problem of versioned formulae

# TODO: licenses is in source.json, but we need a long-term mapping solution

[.[] | 
.name as $name | 
{
    version: .versions.stable, 
    import_id: .name
} | 
"INSERT INTO versions (version, import_id, package_id) VALUES (
  '" + .version + "',
  '" + .import_id + "', 
  (SELECT id FROM packages WHERE import_id = '" + .import_id + "')
  ) ON CONFLICT DO NOTHING;"
] | join("\n")
