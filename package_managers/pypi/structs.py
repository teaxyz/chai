from enum import IntEnum

class DependencyType(IntEnum):
    """Enum for PyPI dependency types."""
    RUNTIME = 1  # Default runtime dependency
    DEV = 2      # Development dependency
    OPTIONAL = 3  # Optional dependency (e.g., extras)

    def __str__(self):
        return self.name.lower()
