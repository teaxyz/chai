
[.[] | 
  "INSERT INTO packages (name, derived_id, import_id, package_manager_id) VALUES ('" +
  # for every single row, extract the name => it's the only key we need from Homebrew
  (.name) + "', '" +
  # the derived_id is the package manager name + "/" + the package name, which enforces
  # uniqueness on the packages table
  ("homebrew/" + .name) + "', '" +
  # the import_id is the same as the package name (used for joins)
  .name + "', '" +
  # the package manager ID is passed in as a variable
  $package_manager_id + "') ON CONFLICT DO NOTHING;"
] | join("\n")
