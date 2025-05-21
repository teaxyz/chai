import json
import re
from typing import List

from permalint import normalize_url
from requests import get

from core.config import Config
from core.logger import Logger
from package_managers.homebrew.structs import Actual

logger = Logger("homebrew_formulae")


def homebrew_api(config: Config) -> List[Actual]:
    """Get the current state of Homebrew"""
    response = get(config.pm_config.source)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching Homebrew formulae: {e}")
        raise e

    # make json
    data = response.json()

    # prep results
    results: List[Actual] = []

    for formula in data:
        # check if deprecated
        # TODO: should we delete
        deprecated = formula.get("deprecated", False)
        if deprecated:
            continue

        # create temp vars for stuff we transform...basically URL
        homepage = normalize_url(formula["homepage"])

        # try urls.head.url, because that generally points to GitHub / git
        # use urls.stable.url as a backstop
        source = normalize_url(
            formula["urls"].get("head", formula["urls"]["stable"]).get("url", "")
        )

        # collect github / gitlab repos
        if re.search(r"^github.com", source) or re.search(r"^gitlab.com", source):
            repository = source
        else:
            repository = None

        # create the actual
        actual = Actual(
            formula=formula["name"],
            description=formula["desc"],
            license=formula["license"],
            homepage=homepage,
            source=source,
            repository=repository,
            build_dependencies=formula["build_dependencies"],
            dependencies=formula["dependencies"],
            test_dependencies=formula["test_dependencies"],
            recommended_dependencies=formula["recommended_dependencies"],
            optional_dependencies=formula["optional_dependencies"],
            # TODO: anything else?
        )

        results.append(actual)

    if config.exec_config.no_cache:
        logger.log("No cache, so not saving to file")
    else:
        with open("homebrew_formulae.json", "w") as f:
            json.dump(results, f)

    return results
