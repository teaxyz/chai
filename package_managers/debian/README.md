# Debian

## Data Structure

- Source represents the original upstream as Debian receives
- Package is a binary that users can install
- Sources can specify multiple binaries
- All packages need not specify a source (transitory or virtual packages)

## Scripts

- `investigate_sources.py` can be run on the downloaded data dump from Debian, and
  prints information about the data integrity

## Approach

There is a many to one mapping between Packages and Sources. During the load step, we
populate the map between Packages and Sources (as in @investigate_sources), because
information about a Debian package can be fetched from both data sources. While the
parser currently captures all the information for each Package and Source (keep as-is),
we only end up loading the following information for a package from each source:

Source:

- Vcs-Browser => URL, PackageURL
- Vcs-Git => URL, PackageURL
- Build-Depends => LegacyDependency
- Maintainer => User, UserPackage
- Uploaders => User, UserPackage

Package:

- Depends => LegacyDependency
- Pre-Depends => LegacyDependency
- Description => Package
- Homepage => URL, PackageURL
