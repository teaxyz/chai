from typing import List, Dict


def safe_int(val: str) -> int | None:
    if val == "":
        return None
    return int(val)


# given some items and a cache, this returns a list of attributes that are not in the
# cache so that we can use them in a query
# attr has to be an attribute in the item
# item[attr] is a key in the cache
def build_query_params(
    items: List[Dict[str, str]], cache: dict, attr: str
) -> List[str]:
    params = set()
    for item in items:
        if item[attr] not in cache:
            params.add(item[attr])
    return list(params)
