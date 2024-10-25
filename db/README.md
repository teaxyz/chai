# CHAI Data Model

The CHAI data model is designed to respresnt the package manager data in a unified and
consistent form. The model's goal is _standardization_ - of the various complexities,
and idiosyncracies of each individual package manager. We want to provide a standard way
for analysis, querying, and whatever your use case might be.

## Definitions

We use certain nomenclature throughout the codebase:

- `derived_id`: A unique identifier combining the package manager and package name. Like
  `crates/serde`, or `homebrew/a2ps`, or `npm/lodash`.
- `import_id`: The original identifier from the source system. Like the `crate_id`
  integers provided by crates, or the package name provided by Homebrew

# Core Entities

## Packages

The Package model is a fundamental unit in our system. Each package is uniquely
identified and associated with a specific package manager.

Key fields:

- `derived_id`
- `name`
- `package_manager_id`: Reference to the associated package manager.
- `import_id`: The original identifier from the source system.
- `readme`: Optional field for package documentation.

### Versions

Each version is a different release of a package, and **must** be associated with a
package.

Key fields:

- `package_id`: Reference to the associated package.
- `version`: The version string.
- `import_id`: The original identifier from the source system.
- `size`, `published_at`, `license_id`, `downloads`, `checksum`: Optional metadata
  fields.

### Users

The User model represents individuals or entities associated with packages. This is not
necessarily always available, but if it is, it's interesting data.

Key fields:

- `username`: The user's name or identifier.
- `source_id`: Reference to the data source (e.g., GitHub, npm user, crates user, etc).
- `import_id`: The original identifier from the source system.

### URLs

The URL model is populated with all the URLs that are provided by the package manager
source data - this includes documentation, repository, source, issues, and other url
types as well. Each URL is associated with a URL type. The relationships between a URL
and a Package are captured in the PackageURL model.

Key fields:

- `url`: The URL.
- `url_type_id`: Reference to the type of URL. (e.g., homepage, repository, etc)

## Type Models

These models define categorizations and types used across the system. All these values
are loaded from the alembic service, specifically in the
[load-values.sql](../alembic/versions/load-values.sql) script.

### URLType

Represents different types of URLs associated with packages.

Predefined types (from load-values.sql):

- `source`
- `homepage`
- `documentation`
- `repository`

### DependsOnType

Categorizes different types of dependencies between packages.
Predefined types (from load-values.sql):
build

- `development`
- `runtime`
- `test`
- `optional`
- `recommended`
- `uses_from_macos` (Homebrew only)

### Source

Represents the authoritative sources of package data.

- `crates`
- `homebrew`

The below are not yet supported:

- `npm`
- `pypi`
- `rubygems`
- `github`

## Relationship Models

These models establish connections between core entities.

### DependsOn

In our data model, a specific release depends on a specific package. We include a field
`semver_range`, which would represent the range of dependency releases compatible with
that specific release.

> [!NOTE]
> Not all package managers provide semantic versions. Homebrew does not, for example.
> This is why `semver_range` is optional.
>
> On the other hand, the dependency type is non-optional, and the combination of
> `version_id`, `dependency_id`, and `dependency_type_id` must be unique.

Key fields:

- `version_id`: The version that has the dependency.
- `dependency_id`: The package that is depended upon.
- `dependency_type_id`: The type of dependency.
- `semver_range`: The version range for the dependency (optional).

### UserVersion and UserPackage

These models associate users with specific versions and packages, respectively.

### PackageURL

Associates packages with their various URLs.

## Caveats

### `Source` and `PackageManager` Relationship

We've chosen to separate `Source` and `PackageManager` into distinct entities:

- `Source`: Represents data sources that can provide information about packages, users,
  or both.
- `PackageManager`: Specifically represents sources that are package managers.

For example, 'crates' functions both as a package manager and as a source of user data.
By keeping these concepts separate, we can accurately represent such systems, and have
one point where we can modify any information about 'crates'.

## Additional Models

### License

Represents software licenses associated with package versions. Great place to start
contirbutions!

### LoadHistory

Tracks the history of data loads for each package manager, useful for auditing and
incremental updates.
