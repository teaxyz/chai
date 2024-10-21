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
