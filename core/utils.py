import re
from os import getenv
from typing import Any, Dict, List


def safe_int(val: str) -> int | None:
    if val == "":
        return None
    return int(val)


# TODO: needs explanation or simplification
def build_query_params(
    items: List[Dict[str, str]], cache: dict, attr: str
) -> List[str]:
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
def convert_keys_to_snake_case(data: Any) -> Any:
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
