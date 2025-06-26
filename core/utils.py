from os import getenv
from os.path import exists, join
from typing import Any


def safe_int(val: str) -> int | None:
    if val == "":
        return None
    return int(val)


# TODO: needs explanation or simplification
def build_query_params(
    items: list[dict[str, str]], cache: dict, attr: str
) -> list[str]:
    params = set()
    for item in items:
        if item[attr] not in cache:
            params.add(item[attr])
    return list(params)


# env vars could be true or 1, or anything else -- here's a centralized location to
# handle that
def env_vars(env_var: str, default: str) -> bool:
    var = getenv(env_var, default).lower()
    return var == "true" or var == "1"


# convert keys to snake case
def convert_keys_to_snake_case(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively converts dictionary keys from hyphen-case to snake_case."""
    if isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            new_key = key.replace("-", "_")
            new_dict[new_key] = convert_keys_to_snake_case(value)  # handle nested
        return new_dict
    elif isinstance(data, list):
        return [convert_keys_to_snake_case(item) for item in data]
    else:
        return data


def is_github_url(url: str) -> bool:
    """Assumes the url has been canonicalized by permalint"""
    return url.startswith("github.com/")


def file_exists(*args) -> str:
    """Confirms if a file exists"""
    file_path = join(*args)
    if not exists(file_path):
        raise FileNotFoundError(f"{file_path} not found")
    return file_path
