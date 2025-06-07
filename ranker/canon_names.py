#!/usr/bin/env uv run --with permalint==0.1.12
from permalint import possible_names


def compute_canon_name(url: str, package_name: str, existing_name: str = "") -> str:
    """
    Determines the name of the canon, based on the package name, URL, and canon name

    Notes:
      - the logic for determining whether it's an update or not, is left to the caller
      - this function does not do anything for monorepos
      - as a fallback, the original package name is always returned
    """
    if not url or not package_name:
        raise ValueError(f"Missing one of url={url} | package_name={package_name}")

    best_guess = extract_repo_name_from_url(url)

    if existing_name:
        return check_if_we_have_something_better(
            best_guess, package_name, existing_name
        )

    return package_name


def check_if_we_have_something_better(
    best_guess: str, package_name: str, existing_name: str
) -> str:
    """Check if we have a better name than the existing name."""
    if best_guess == package_name:
        # boom, this is the ideal case. the repo and the package share a name!
        return package_name

    # if not, we need to do some heuristics on the package_name to determine if it's
    # better than our best_guess
    # remember, best_guess is a guess at its name. our fallback will **always** be the
    # original name, not the best_guess.

    package_name_score = score_name(package_name, best_guess)
    existing_name_score = score_name(existing_name, best_guess)

    if package_name_score > existing_name_score:
        return package_name

    return existing_name


def extract_repo_name_from_url(url: str) -> str:
    """
    Extract a reasonable name from a URL, typically the repository name.

    We're trusting permalint's rules for guessing a package's name based on
    the homepage URL here. Note that the fallback is always to retrieve the full URL
    name, which will be the only element in the result
    """
    if not url:
        return url

    names: list[str] = possible_names(url)
    if len(names) > 1:
        return names[1].lower()
    else:
        return names[0].lower()


def score_name(name: str, best_guess: str) -> int:
    """
    Score a package name based on some rules

    1. Prefer shorter, simpler names
    2. Prefer names without prefixes/suffixes that suggest forks or variations
    """
    if not name and not best_guess:
        raise ValueError(f"Missing one of name={name} | guess={best_guess}")

    score = 0
    clean = name.lower()

    if best_guess in clean:
        score += 1

    # Prefer shorter names
    score += max(0, 20 - len(clean))

    # Penalize scoped packages
    if clean.startswith("@"):
        score -= 3

    return score
