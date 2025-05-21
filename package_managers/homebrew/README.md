# Homebrew

The Homebrew service uses Homebrew's JSON API Documentation to build the Homebrew
data model, using a diff approach to build it out.

## Getting Started

To just run the Homebrew service, you can use the following commands:

```bash
docker compose build homebrew
docker compose run homebrew
```

## Pipeline Overview

The Homebrew pipeline consists of one main script: `main.py`. It fetches two things
from CHAI:

1. Homebrew's Graph, based on packages and legacy dependencies
2. All the URLs in CHAI for Homebrew's clean URLs

Based on that, it does a diff across each object for each package, and makes changes to
CHAI accordingly.

## Notes

- Homebrew's dependencies are not just restricted to the `{build,test,...}_dependencies`
  fields listed in the JSON APIs...it also uses some system level packages denoted in
  `uses_from_macos`, and `variations` (for linux). The pipeline currently does NOT
  consider those dependencies
- This job ignores the versions table entirely, and instead populates the legacy
  dependencies table, which maintains a package to package relationship
- Versioned formulae (like `python`, `postgresql`) are ones where the Homebrew package
  specifies a version. The pipeline considers these packages individual packages,
  and so creates new records in the `packages` table.
- The data source for Homebrew does not retrieve the analytics information that is
  available via the individual JSON API endpoints for each package.
