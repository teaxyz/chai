from dataclasses import dataclass


@dataclass
class Actual:
    formula: str
    description: str
    license: str
    homepage: str
    source: str
    repository: str | None
    build_dependencies: list[str] | None
    dependencies: list[str] | None
    test_dependencies: list[str] | None
    recommended_dependencies: list[str] | None
    optional_dependencies: list[str] | None
