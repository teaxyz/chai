# build_dependencies
# dependencies
# test_dependencies
# optional_dependencies
# uses_from_macos
# TODO: variations (linux only, by architecture)
# all of the above are the fields that contain dependency info for Homebrew

# uses from macos sometimes specifies build / test -- right now logging that as macos only

[.[] | 
.name as $name |
(
  (.uses_from_macos // []) | 
  map({
    package: $name,
    dependency_type: "uses_from_macos",
    dependency: (if type == "object" then keys[0] else . end)
  })
),
(
  (.dependencies // []) | 
  map({package: $name, dependency_type: "dependency", dependency: .})
),
(
  (.test_dependencies // []) | 
  map({package: $name, dependency_type: "test_dependency", dependency: .})
),
(
  (.optional_dependencies // []) | 
  map({package: $name, dependency_type: "optional_dependency", dependency: .})
),
(
  (.build_dependencies // []) | 
  map({package: $name, dependency_type: "build_dependency", dependency: .})
)
| .[]]