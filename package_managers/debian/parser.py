import re
from collections.abc import Iterator

from permalint import normalize_url

from package_managers.debian.structs import DebianData, Depends, Maintainer


# NOTE: The DebianParser is the one which normalizes all the URLs!
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

            # State for handling multiline fields
            current_field = None
            current_value = ""

            # populate the object
            lines = paragraph.split("\n")
            for _i, line in enumerate(lines):
                # if the line is empty, then move on
                if not line.strip():
                    continue

                # if the line starts with a tab or space, then it's a continuation of
                # the previous field
                if line[0] == " " or line[0] == "\t":
                    # Append continuation line to current field value
                    if current_field is not None:
                        current_value += " " + line.strip()
                    continue

                # Process any accumulated field before starting new one
                if current_field is not None:
                    self.mapper(obj, current_field, current_value)

                # Start new field
                if ":" not in line:
                    continue

                key, value = line.split(":", 1)
                current_field = key.strip()
                current_value = value.strip()

            # Process the final accumulated field
            if current_field is not None:
                self.mapper(obj, current_field, current_value)

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
                obj.homepage = normalize_url(value.strip())
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
                obj.vcs_browser = normalize_url(value.strip())
            case "Vcs-Git":
                obj.vcs_git = normalize_url(value.strip())
            case "Directory":
                obj.directory = value.strip()
            case "Testsuite":
                obj.testsuite = value.strip()
            case "Testsuite-Triggers":
                obj.testsuite_triggers = value.strip()
            case "Binary":
                obj.binary = [bin.strip() for bin in value.split(",") if bin.strip()]
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
                for dependency in dependencies:
                    obj.recommends.append(handle_depends(dependency.strip()))
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
                # Split by comma but respect quoted sections
                uploaders = []
                in_quotes = False
                current = ""

                for char in value:
                    if char == '"':
                        in_quotes = not in_quotes
                        current += char
                    elif char == "," and not in_quotes:
                        if current.strip():
                            uploaders.append(current.strip())
                        current = ""
                    else:
                        current += char

                if current.strip():
                    uploaders.append(current.strip())

                for uploader in uploaders:
                    obj.uploaders.append(handle_maintainer(uploader.strip()))
            case "Maintainer":
                obj.maintainer = handle_maintainer(value.strip())

            # TODO: File Fields
            case _:
                pass


# Helpers for handling specific fields in the mapper
def handle_depends(dependency: str) -> Depends:
    # Handle various dependency formats:
    # 0ad-data (>= 0.0.26)
    # lib32gcc1-amd64-cross [amd64 arm64 i386 ppc64el x32]
    # gm2-11 [!powerpc !ppc64 !x32]
    # debhelper-compat (= 13)
    # gcc-11-source (>= 11.3.0-11~)

    # First, strip platform specifications in square brackets
    # Remove platform specs like [amd64 arm64 i386 ppc64el x32] or [!powerpc !ppc64 !x32]
    platform_match = re.search(r"\s*\[[^\]]+\]", dependency)
    if platform_match:
        dependency = dependency.replace(platform_match.group(0), "").strip()

    # Now handle version constraints in parentheses
    match = re.match(r"^(.*?)(\s*\((.*)\))?$", dependency)
    if match:
        dep = match.group(1).strip()
        if match.group(2):
            semver = match.group(3)
            return Depends(package=dep, semver=semver)
        else:
            return Depends(package=dep, semver="*")
    raise ValueError(f"Invalid dependency: {dependency}")


def handle_maintainer(value: str) -> Maintainer:
    # Remove trailing comma if present
    value = value.rstrip(",")

    # For names with quotes like "Adam C. Powell, IV" <hazelsct@debian.org>
    if '"' in value:
        match = re.match(r'^"([^"]*)" <([^>]*)>$', value)
        if match:
            return Maintainer(name=match.group(1), email=match.group(2))

    # Standard format: Name <email@example.com>
    match = re.match(r"^(.*) <([^>]*)>$", value)
    if match:
        return Maintainer(name=match.group(1), email=match.group(2))

    raise ValueError(f"Invalid maintainer: {value}")
