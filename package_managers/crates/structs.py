from enum import IntEnum


class DependencyType(IntEnum):
    NORMAL = 0
    BUILD = 1  # used for build scripts
    DEV = 2  # used for testing or benchmarking
    OPTIONAL = 3

    def __str__(self):
        return self.name.lower()
