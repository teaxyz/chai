# build_dependencies
# dependencies
# test_dependencies
# optional_dependencies
# uses_from_macos
# variations

.[] | 
.name as $name |
(
  (.uses_from_macos // []) | 
  map({package: $name, dependency_type: "uses_from_macos", dependency: .})
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
| .[]