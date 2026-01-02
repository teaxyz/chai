import re
from uuid import UUID

from permalint import normalize_url, possible_names
from requests import Response, get

from core.config import Config
from core.logger import Logger
from core.structs import URLKey
from core.utils import is_github_url
from package_managers.pkgx.db import DB

HOMEPAGE_URL = "https://pkgx.dev/pkgs/{name}.json"


def canonicalize(url: str) -> str:
    return normalize_url(url)


def guess(db_client: DB, package_managers: list[UUID], url: str) -> list[str]:
    names = possible_names(url)
    urls = db_client.search_names(names, package_managers)
    return urls


def ask_pkgx(import_id: str) -> str | None:
    """
    ask max's scraping work for the homepage of a package
    Homepage comes from the pkgxdev/www repo
    The API https://pkgx.dev/pkgs/{name}.json returns a blob which may contain
    the homepage field
    """
    response: Response = get(HOMEPAGE_URL.format(name=import_id))
    if response.status_code == 200:
        data: dict[str, str] = response.json()
        if "homepage" in data:
            return data["homepage"]


def special_case(import_id: str, logger: Logger) -> str | None:
    homepage: str | None = None

    # if no slashes, then pkgx used the homepage as the name
    # if two slashes, then probably github / gitlab
    if not re.search(r"/", import_id) or re.search(r"/.+/", import_id):
        homepage = import_id

    # if it's a crates.io package, then we can use the crates URL
    elif re.search(r"^crates.io", import_id):
        if "/" in import_id:
            name = import_id.split("/")[1]
            homepage = f"https://crates.io/crates/{name}"
        else:
            logger.warn(f"Invalid format for crates.io import_id: {import_id}")

    # if it's part of the x.org family
    elif re.search(r"^x.org", import_id):
        homepage = "https://x.org"

    # if it's part of the pkgx family
    elif re.search("^pkgx.sh", import_id):
        tool = import_id.split("/")[1]
        homepage = f"https://github.com/pkgxdev/{tool}"

    # python.org/typing_extensions
    elif import_id == "python.org/typing_extensions":
        homepage = "https://github.com/python/typing_extensions"

    # thrysoee.dk/editline
    elif import_id == "thrysoee.dk/editline":
        homepage = "https://thrysoee.dk/editline"

    # gen-ir is a Homebrew Tap, which lists this as its homepage
    elif import_id == "veracode.com/gen-ir":
        homepage = "https://github.com/veracode/gen-ir"

    else:
        logger.warn(f"no homepage in pkgx for {import_id}")

    return homepage


def generate_chai_urls(
    config: Config, db: DB, import_id: str, distributable_url: str, logger: Logger
) -> list[URLKey]:
    """For a pkgx import_id, generate a list of URLs it could have"""
    urls: list[URLKey] = []

    # homepage
    similar = [config.package_managers.debian, config.package_managers.homebrew]
    maybe: list[str] = guess(db, similar, import_id)

    if maybe:
        homepage = maybe[0]
    else:
        homepage = ask_pkgx(import_id)

        if not homepage:
            homepage = special_case(import_id, logger)

    if homepage:
        canonical_homepage = canonicalize(homepage)
        urls.append(URLKey(canonical_homepage, config.url_types.homepage))

    # source
    # NOTE: for non-GitHub source URLs, pkgx tells you where the version string for the
    # downloadable tarball is...right now, we don't do anything about that
    canonical_distributable = canonicalize(distributable_url)
    urls.append(URLKey(canonical_distributable, config.url_types.source))

    if is_github_url(canonical_distributable):
        urls.append(URLKey(canonical_distributable, config.url_types.repository))

    return urls
