from os import getenv
from typing import Dict, List


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
def env_vars(env_var: str, default: str):
    var = getenv(env_var, default).lower()
    return var == "true" or var == "1"
