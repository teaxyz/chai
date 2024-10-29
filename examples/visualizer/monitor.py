import argparse
import time
from functools import wraps
from typing import Callable

from main import DB, main


class MonitoredDB(DB):
    """Extends DB class to monitor database operations"""

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

    def connect(self) -> None:
        super().connect()
        # Wrap all query methods with monitoring
        self.select_id = self._monitor_query(self.select_id)
        self.select_name = self._monitor_query(self.select_name)
        self.select_deps = self._monitor_query(self.select_deps)
        self.select_deps_v2 = self._monitor_query(self.select_deps_v2)


def run_monitored(package: str) -> dict[str, float]:
    """Run the main program with monitoring"""
    db = MonitoredDB()

    start_time = time.perf_counter()
    main(db, package)
    total_time = time.perf_counter() - start_time

    return {
        "total_execution_time": total_time,
        "query_count": db.query_count,
        "total_query_time": db.total_query_time,
        "non_query_time": total_time - db.total_query_time,
    }


def compare_runs(package: str, runs: int = 3) -> None:
    """Run multiple times and average the results"""
    results = []
    for i in range(runs):
        print(f"\nRun {i + 1}/{runs}")
        results.append(run_monitored(package))

    # Calculate averages
    avg_results = {
        key: sum(r[key] for r in results) / len(results) for key in results[0].keys()
    }

    # Print results
    print("\nAverage Results:")
    print("-" * 50)
    print(f"Total Execution Time: {avg_results['total_execution_time']:.3f}s")
    print(f"Number of Queries: {avg_results['query_count']:.0f}")
    print(f"Total Query Time: {avg_results['total_query_time']:.3f}s")
    print(f"Non-Query Time: {avg_results['non_query_time']:.3f}s")
    print(
        f"Query Time Percentage: {(avg_results['total_query_time'] / avg_results['total_execution_time'] * 100):.1f}%"  # noqa
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("package", help="The package to visualize")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs to average")
    args = parser.parse_args()

    compare_runs(args.package, args.runs)
