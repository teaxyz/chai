#!/usr/bin/env uv run --with permalint==0.1.12

from typing import Optional

from permalint import possible_names

from core.logger import Logger


def compute_canon_name_v2(url: str, package_name: str, existing_name: str = "") -> str:
    """
    In general:

    1. It always compares an existing name with a new potential name
    2. It is given a homepage URL, an existing name, and an optional existing canon name
    3. It computes a best name, and if different from the existing name, it returns the
    new name. Otherwise, it returns the existing name.
    NOTE: the logic for determining whether it's an update or not, is left to the caller
    4. It DOES NOT do anything for monorepos.
    5. As a fallback, it always returns the canon URL as the name.

    Also, if we have a reason to pick the package registry name, let's do that, rather
    than select our best guess
    """
    if not url or not package_name:
        raise ValueError(
            "URL / package name are required: url={url}, package_name={package_name}"
        )

    best_guess = extract_repo_name_from_url(url)

    if existing_name:
        return check_if_we_have_something_better(
            best_guess, package_name, existing_name
        )
    else:
        return new_name(best_guess, package_name, url)


def check_if_we_have_something_better(
    best_guess: str, package_name: str, existing_name: str
) -> str:
    """
    Check if we have a better name than the existing name.
    """
    # TODO: implement this
    return existing_name


def new_name(best_guess: str, package_name: str, url: str) -> str:
    """
    Compute a new name for a canon.
    """

    if best_guess == package_name:
        # boom, this is the ideal case. the repo and the package share a name!
        return package_name

    # if not, we need to do some heuristics on the package_name to determine if it's
    # better than our best_guess
    # remember, best_guess is a guess at its name. our fall back will **always** be the
    # homepage URL, not the best_guess. so, it's unlikely that we return that

    # we don't like scoped packages, because they are generally forks of something else
    if package_name.startswith("@"):
        unscoped_name: str = package_name.split("/")[-1]

        # if our best guess is in the unscoped name, we can use package name, else
        # return the homepage URL
        if best_guess in unscoped_name:
            return package_name
        else:
            return url

    # if the best guess is inside the package name, let's use that one
    if best_guess in package_name:
        return package_name

    # fallback to the URL name
    return url


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


def find_best_package_name(package_names: list[str], url: str) -> Optional[str]:
    """
    Find the best package name from a list of candidates.

    This implements heuristics to pick the most "canonical" name:
    1. Prefer names that match the repo name from URL
    2. Prefer shorter, simpler names
    3. Prefer names without prefixes/suffixes that suggest forks or variations
    """
    if not package_names:
        return None

    repo_name = extract_repo_name_from_url(url)

    # First, look for exact matches with the repo name
    exact_matches = [name for name in package_names if name == repo_name]
    if exact_matches:
        return exact_matches[0]

    # Score each name based on various heuristics
    scored_names = []
    for name in package_names:
        score = 0

        # Prefer names that contain the repo name
        if repo_name.lower() in name.lower():
            score += 10

        # Prefer shorter names
        score += max(0, 20 - len(name))

        # Penalize names with common fork/variant indicators
        fork_indicators = ["@", "-", "_component", "_fork", "my-", "custom-"]
        for indicator in fork_indicators:
            if indicator in name.lower():
                score -= 5

        # Penalize scoped packages (they're often forks or organization-specific)
        if name.startswith("@"):
            score -= 3

        scored_names.append((score, name))

    # Sort by score (highest first), then by name length (shortest first)
    scored_names.sort(key=lambda x: (-x[0], len(x[1])))

    return scored_names[0][1] if scored_names else package_names[0]


def compute_canon_name(
    current_package_name: str,
    canon_url: str,
    existing_canon_name: Optional[str],
    packages_for_canon: list[str],
    logger: Optional[Logger] = None,
) -> str:
    """
    Compute the best canonical name for a canon based on current context.

    Args:
        current_package_name: Name of the current package being processed
        canon_url: The canonical URL for this canon
        existing_canon_name: Current name of the canon (None for new canons)
        packages_for_canon: List of all package names that point to this canon
        logger: Optional logger for debugging

    Returns:
        The best canonical name to use for this canon

    Logic:
        1. If >20 packages point to this canon (monorepo), use URL as name
        2. For smaller groups, try to find the best package name using heuristics
        3. Fall back to URL if no good name can be determined
    """
    if logger:
        logger.debug(
            f"Computing canon name for {current_package_name}, "
            f"URL: {canon_url}, existing: {existing_canon_name}, "
            f"packages: {len(packages_for_canon)}"
        )

    # Include current package in the list if not already there
    all_packages = list(set(packages_for_canon + [current_package_name]))

    # Monorepo detection: if >20 packages, use URL as name
    if len(all_packages) > 20:
        if logger:
            logger.debug(
                f"Detected monorepo with {len(all_packages)} packages, using URL as name"
            )
        return canon_url

    # For smaller groups, try to find the best name
    best_name = find_best_package_name(all_packages, canon_url)

    if best_name:
        if logger:
            logger.debug(f"Selected '{best_name}' as best name from {all_packages}")
        return best_name

    # Fallback to URL
    if logger:
        logger.debug(f"Falling back to URL as name: {canon_url}")
    return canon_url
