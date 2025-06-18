from dataclasses import dataclass

from core.structs import URLKey


@dataclass
class PkgxURLs:
    homepage: URLKey | None = None
    source: URLKey | None = None
    repository: URLKey | None = None
