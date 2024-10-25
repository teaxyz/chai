# CHAI Data Model

Include a write up about each of the data models. Keep a specific section for the `type`
data models, including `URLType`, `DependencyType`, and `UserType`. Talk about the raw
data values that are loaded from the alembic [load script](../alembic/load-values.sql).
Additionally, keep a separate section for the relationship between `Source` and
`PackageManager`, highlighting we chose to keep those as separate. The rationale was to
maintain one position where all our data sources are covered; since `crates` can
function as a data source for packages, as well as a data source for users.
