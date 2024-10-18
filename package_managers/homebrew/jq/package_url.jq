[.[] | {
  package_name: .name,
  homepage_url: .homepage,
  source_url: .urls.stable.url
} | [
  {package_name: .package_name, url: .homepage_url},
  {package_name: .package_name, url: .source_url}
] | .[]]
