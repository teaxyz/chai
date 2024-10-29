import argparse
import time
from collections import defaultdict
from functools import wraps
from typing import Callable

from main import DB, main, old_main


class Result:
    METRICS = [
        "total_execution_time",
        "query_count",
        "total_query_time",
        "non_query_time",
    ]

    def __init__(self, **kwargs):
        for metric in self.METRICS:
            setattr(self, metric, kwargs[metric])

    def __str__(self):
        return "\n".join(
            f"{metric}: {getattr(self, metric):.3f}s"
            if metric != "query_count"  # I don't like this
            else f"{metric}: {getattr(self, metric)}"
            for metric in self.METRICS
        )


class MonitoredDB(DB):
    """Base monitoring wrapper for DB classes"""

    def __init__(self):
        self.query_count = 0
        self.total_query_time = 0
        super().__init__()

    def _monitor_query(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.query_count += 1
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            self.total_query_time += time.perf_counter() - start_time
            return result

        return wrapper

    def connect(self):
        super().connect()
        # and wrap all the methods with monitoring
        for name in dir(self):
            if name.startswith("select_"):
                setattr(self, name, self._monitor_query(getattr(self, name)))


def run_monitored(func: Callable, package: str) -> Result:
    """Run the main program with monitoring"""
    db = MonitoredDB()
    start_time = time.perf_counter()
    func(db, package)
    total_time = time.perf_counter() - start_time

    return Result(
        total_execution_time=total_time,
        query_count=db.query_count,
        total_query_time=db.total_query_time,
        non_query_time=total_time - db.total_query_time,
    )


def compare_implementations(package: str, runs: int = 3) -> dict[str, list[Result]]:
    """Compare old and new implementations"""
    implementations = [main, old_main]
    results: dict[str, list[Result]] = defaultdict(list)

    for i in range(runs):
        print(f"\nRun {i + 1}/{runs}")
        for func in implementations:
            func_name = func.__name__
            print(f"Running {func_name}...")
            result = run_monitored(func, package)
            results[func_name].append(result)

    return results


def compare_results(results: dict[str, list[Result]], runs: int) -> None:
    implementations = list(results.keys())

    print("\nResults Comparison:")
    print("-" * (25 + 20 * len(implementations)))

    # Header row with implementation names
    print(f"{'Metric':<25}", end="")
    for impl in implementations:
        print(f"{impl:>20}", end="")
    print()
    print("-" * (25 + 20 * len(implementations)))

    # Data rows
    for metric in Result.METRICS:
        print(f"{metric:<25}", end="")
        for impl in implementations:
            avg = sum(getattr(r, metric) for r in results[impl]) / runs
            if metric == "query_count":
                print(f"{avg:>20.0f}", end="")
            else:
                print(f"{avg:>20.3f}s", end="")
        print()

    # Calculate improvements relative to first implementation
    print("-" * (25 + 20 * len(implementations)))
    base_time = sum(r.total_execution_time for r in results[implementations[0]]) / runs
    for impl in implementations[1:]:
        new_time = sum(r.total_execution_time for r in results[impl]) / runs
        improvement = ((base_time - new_time) / base_time) * 100
        print(f"Improvement ({impl} vs {implementations[0]}): {improvement:>+.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("package", help="The package to visualize")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs to average")
    args = parser.parse_args()

    results = compare_implementations(args.package, args.runs)
    compare_results(results, args.runs)
