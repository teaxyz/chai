# homebrew has the problem where there are no versions
# we're gonna assume the version available is the latest
# and we'll deal with that later

# TODO: `downloads: .analytics.install_on_request."365d".[$name]`
# above gives us the downloads for the last 365 days
# not available in the full JSON API

# TODO: there are also a problem of versioned formulae
.[] | 
.name as $name | 
{
    version: .versions.stable, 
    import_id: .name, 
    license: .license
}