# Homebrew

## Notes

- Homebrew's dependencies are not just restricted to the `{build,test,...}_dependencies`
  fields listed in the JSON APIs...it also uses some system level packages denoted in
  `uses_from_macos`, and `variations` (for linux). The pipeline currently does consider
  these dependencies.
- Homebrew's JSON API and formula.rb files do not specify all the versions available for
  a package. It does provide the `stable` and `head` versions, which are pulled in
  [`versions.jq`](jq/versions.jq).
- Versioned formulae (like `python`, `postgresql`) are ones where the Homebrew package
  specifies a version. The pipeline considers these packages individual packages,
  and so creates new records in the `packages` table.
- The data source for Homebrew does not retrieve the analytics information that is
  available via the individual JSON API endpoints for each package.
