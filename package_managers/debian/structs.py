from dataclasses import dataclass, field


# structures
@dataclass
class Maintainer:
    name: str = field(default_factory=str)
    email: str = field(default_factory=str)


@dataclass
class File:
    hash: str = field(default_factory=str)
    size: int = field(default_factory=int)
    filename: str = field(default_factory=str)


@dataclass
class Depends:
    package: str = field(default_factory=str)
    semver: str = field(default_factory=str)


@dataclass
class Tag:
    name: str = field(default_factory=str)
    value: str = field(default_factory=str)


# this represents whatever we might get from Debian...either packages or sources
# it's immaterial what it is, we just need to know how to parse it
@dataclass
class DebianData:
    # Package fields
    package: str = field(default_factory=str)
    source: str = field(default_factory=str)
    version: str = field(default_factory=str)
    installed_size: int = field(default_factory=int)
    maintainer: Maintainer = field(default_factory=Maintainer)
    architecture: str = field(default_factory=str)
    description: str = field(default_factory=str)
    homepage: str = field(default_factory=str)
    description_md5: str = field(default_factory=str)
    tag: str = field(default_factory=str)
    section: str = field(default_factory=str)
    priority: str = field(default_factory=str)
    filename: str = field(default_factory=str)
    size: int = field(default_factory=int)
    md5sum: str = field(default_factory=str)
    sha256: str = field(default_factory=str)

    # Dependency fields
    replaces: list[Depends] = field(default_factory=list)
    provides: list[Depends] = field(default_factory=list)
    depends: list[Depends] = field(default_factory=list)
    pre_depends: list[Depends] = field(default_factory=list)
    recommends: list[Depends] = field(default_factory=list)
    suggests: list[Depends] = field(default_factory=list)
    breaks: list[Depends] = field(default_factory=list)
    conflicts: list[Depends] = field(default_factory=list)
    build_depends: list[Depends] = field(default_factory=list)  # source only

    # Source fields
    binary: list[str] = field(default_factory=list)
    uploaders: list[Maintainer] = field(default_factory=list)
    standards_version: str = field(default_factory=str)
    format: str = field(default_factory=str)
    files: list[File] = field(default_factory=list)
    vcs_browser: str = field(default_factory=str)
    vcs_git: str = field(default_factory=str)
    checksums_sha256: list[File] = field(default_factory=list)
    package_list: list[str] = field(default_factory=list)
    directory: str = field(default_factory=str)
    testsuite: str = field(default_factory=str)
    testsuite_triggers: str = field(default_factory=str)
