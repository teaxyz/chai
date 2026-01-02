#!/usr/bin/env pkgx +python@3.11 uv run

"""
Parse graph run log to calculate processing metrics.

This script analyzes a log file to compute:
1. Average time to process 1,000 packages
2. Average packages processed per second

Usage:
  From file:    ./parse_log.py log_file
  From tmux:    tmux capture-pane -p | ./parse_log.py
"""

import re
import sys
from statistics import mean


def parse_log_line(line: str) -> tuple[float, int]:
    """
    Extract timestamp and package count from a log line.

    Args:
        line: A line from the log file

    Returns:
        Tuple of (timestamp, package_count)
    """
    pattern = r"^(\d+\.\d+): \[graph\.main\]: (\d+):"
    match = re.match(pattern, line)
    if match:
        timestamp = float(match.group(1))
        package_count = int(match.group(2))
        return timestamp, package_count
    return None


def calculate_metrics(log_lines: list[str]) -> tuple[float, float]:
    """
    Calculate processing metrics from log lines.

    Args:
        log_lines: List of log file lines

    Returns:
        Tuple of (avg_time_per_1000, packages_per_second)
    """
    data_points = []
    previous_timestamp = None
    previous_count = None

    for line in log_lines:
        result = parse_log_line(line)
        if not result:
            continue

        timestamp, count = result

        if previous_timestamp is not None and previous_count is not None:
            time_diff = timestamp - previous_timestamp
            count_diff = count - previous_count

            # Only process if we're looking at approximately 1000 package difference
            if 900 <= count_diff <= 1100:
                data_points.append((time_diff, count_diff))

        previous_timestamp = timestamp
        previous_count = count

    if not data_points:
        return 0.0, 0.0

    # Calculate average time for processing 1000 packages
    time_diffs = [time for time, _ in data_points]
    avg_time_per_1000 = mean(time_diffs)

    # Calculate average packages per second
    packages_per_second = 1000 / avg_time_per_1000

    return avg_time_per_1000, packages_per_second


def main():
    """Process the log data and display metrics."""
    log_lines = []

    # Read from file if specified, otherwise from stdin
    if len(sys.argv) == 2:
        log_file = sys.argv[1]
        try:
            with open(log_file) as f:
                log_lines = f.readlines()
        except OSError as e:
            print(f"Error reading log file: {e}")
            sys.exit(1)
    else:
        # Read from stdin (for piping from tmux)
        log_lines = sys.stdin.readlines()
        if not log_lines:
            print(f"Usage: {sys.argv[0]} [log_file]")
            print(f"   or: tmux capture-pane -p | {sys.argv[0]}")
            sys.exit(1)

    avg_time, pkg_per_second = calculate_metrics(log_lines)

    print(f"Average time to process 1,000 packages: {avg_time:.2f} seconds")
    print(f"Average packages processed per second: {pkg_per_second:.2f}")


if __name__ == "__main__":
    main()
