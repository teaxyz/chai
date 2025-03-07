import re
from dataclasses import dataclass, field
from typing import Iterator


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
    build_depends: list[str] = field(default_factory=list)  # source only

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


class DebianParser:
    def __init__(self, content: str):
        # content is the Packages or Sources file
        self.content = content

    def parse(self) -> Iterator[DebianData]:
        """Yield packages and sources from the Packages and Sources files."""
        paragraphs = self.content.split("\n\n")

        # iterate over the lines
        for paragraph in paragraphs:
            # if the paragraph is empty, then move on
            if not paragraph.strip():
                continue

            # each paragraph represents one object
            obj = DebianData()

            # populate the object
            for line in paragraph.split("\n"):
                # if the line is empty, then move on
                if not line.strip():
                    continue

                # if the line starts with a tab or space, then it's a continuation of
                # the previous field
                if line[0] == " " or line[0] == "\t":
                    continue

                # split the line into key and value
                self.handle_line(obj, line)

            if obj.package:
                yield obj
            else:
                raise ValueError(f"Invalid package: {paragraph}")

    def handle_line(self, obj: DebianData, line: str) -> None:
        key, value = line.split(":", 1)
        self.mapper(obj, key, value)

    def mapper(self, obj: DebianData, key: str, value: str) -> None:
        """Map fields from Debian package/source files to DebianData object."""
        match key:
            case "Package":
                obj.package = value.strip()
            case "Source":
                obj.source = value.strip()
            case "Version":
                obj.version = value.strip()
            case "Installed-Size":
                obj.installed_size = int(value.strip())
            case "Architecture":
                obj.architecture = value.strip()
            case "Description":
                obj.description = value.strip()
            case "Homepage":
                obj.homepage = value.strip()
            case "Description-md5":
                obj.description_md5 = value.strip()
            case "Tag":
                obj.tag = value.strip()
            case "Section":
                obj.section = value.strip()
            case "Priority":
                obj.priority = value.strip()
            case "Filename":
                obj.filename = value.strip()
            case "Size":
                obj.size = int(value.strip())
            case "MD5sum":
                obj.md5sum = value.strip()
            case "SHA256":
                obj.sha256 = value.strip()
            case "Standards-Version":
                obj.standards_version = value.strip()
            case "Format":
                obj.format = value.strip()
            case "Vcs-Browser":
                obj.vcs_browser = value.strip()
            case "Vcs-Git":
                obj.vcs_git = value.strip()
            case "Directory":
                obj.directory = value.strip()
            case "Testsuite":
                obj.testsuite = value.strip()
            case "Testsuite-Triggers":
                obj.testsuite_triggers = value.strip()
            case "Binary":
                obj.binary = [bin.strip() for bin in value.split(",")]
            case "Package-List":
                obj.package_list = [pkg.strip() for pkg in value.split(",")]

            # Dependency Fields
            case "Depends":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.depends.append(handle_depends(dependency.strip()))
            case "Pre-Depends":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.pre_depends.append(handle_depends(dependency.strip()))
            case "Replaces":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.replaces.append(handle_depends(dependency.strip()))
            case "Provides":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.provides.append(handle_depends(dependency.strip()))
            case "Recommends":
                dependencies = value.split(", ")
                print(f"******* Recommended dependencies: {dependencies}")
                for dependency in dependencies:
                    obj.recommends.append(handle_depends(dependency.strip()))
                print(f"******* Recommends: {obj.recommends}")
            case "Suggests":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.suggests.append(handle_depends(dependency.strip()))
            case "Breaks":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.breaks.append(handle_depends(dependency.strip()))
            case "Conflicts":
                dependencies = value.split(", ")
                for dependency in dependencies:
                    obj.conflicts.append(handle_depends(dependency.strip()))
            case "Build-Depends":
                for build_depends in value.split(", "):
                    obj.build_depends.append(handle_depends(build_depends.strip()))

            # Maintainer fields
            case "Uploaders":
                for uploader in value.split(", "):
                    obj.uploaders.append(handle_maintainer(uploader.strip()))
            case "Maintainer":
                obj.maintainer = handle_maintainer(value.strip())

            # TODO: File Fields
            case _:
                pass


# Helpers for handling specific fields in the mapper
def handle_depends(dependency: str) -> Depends:
    # 0ad-data (>= 0.0.26)
    # use regex to match the `()`, because a split won't work
    match = re.match(r"^(.*?)(\s*\((.*)\))?$", dependency)
    if match:
        dep = match.group(1)
        if match.group(2):
            semver = match.group(3)
            return Depends(package=dep, semver=semver)
        else:
            return Depends(package=dep, semver="*")
    raise ValueError(f"Invalid dependency: {dependency}")


def handle_maintainer(value: str) -> Maintainer:
    match = re.match(r"^(.*) <(.*)>,?$", value)
    # Debian Games Team <pkg-games-devel@lists.alioth.debian.org>
    # There might be a comma at the end of the name
    if match:
        return Maintainer(name=match.group(1), email=match.group(2))
    raise ValueError(f"Invalid maintainer: {value}")
