import time
from typing import Tuple, Optional, Dict
import docker
import json
from collections import defaultdict

PIPELINE_CONTAINER = "chai-oss-pipeline-1"
DATABASE_CONTAINER = "chai-oss-db-1"


def get_container_stats(
    container: docker.models.containers.Container,
) -> Optional[Dict[str, float]]:
    stats = container.stats(stream=False)
    cpu_stats = stats.get("cpu_stats", {})
    precpu_stats = stats.get("precpu_stats", {})
    memory_stats = stats.get("memory_stats", {})

    # in case it exits with a code of 1
    # TODO: i'm not really sure we need to do this, since the container status
    # should change to exited
    if not cpu_stats or not precpu_stats or not memory_stats:
        return None

    cpu_usage = (
        cpu_stats["cpu_usage"]["total_usage"] - precpu_stats["cpu_usage"]["total_usage"]
    )
    system_cpu_usage = cpu_stats["system_cpu_usage"] - precpu_stats["system_cpu_usage"]
    number_cpus = cpu_stats["online_cpus"]
    cpu_percent = (cpu_usage / system_cpu_usage) * number_cpus * 100.0

    memory_usage = memory_stats["usage"] / (1024 * 1024)  # Convert to MB
    memory_limit = memory_stats["limit"] / (1024 * 1024)  # Convert to MB

    return {
        "cpu_percent": cpu_percent,
        "memory_usage": memory_usage,
        "memory_limit": memory_limit,
    }


def read_logs(logs: str) -> Tuple[int, int, float]:
    select_count = 0
    insert_count = 0
    total_sql_time = 0

    for line in logs.split("\n"):
        if "Executed" in line:
            if "SELECT" in line:
                select_count += 1
            elif "INSERT" in line:
                insert_count += 1
            elif "Execution time:" in line:
                total_sql_time += float(line.split("Execution time:")[1].strip())

    return select_count, insert_count, total_sql_time


def capture_stats(container, start_time):
    stats = get_container_stats(container)
    if stats is None:
        return None
    end_time = time.time()
    return {
        "duration": end_time - start_time,
        "max_cpu_percent": stats["cpu_percent"],
        "max_memory_usage": stats["memory_usage"],
    }


def monitor_pipeline() -> None:
    client = docker.from_env()
    pipeline_container = None
    database_container = None
    start_time = time.time()

    # detect the containers
    while not pipeline_container and not database_container:
        try:
            pipeline_container = client.containers.get(PIPELINE_CONTAINER)
            database_container = client.containers.get(DATABASE_CONTAINER)
        except docker.errors.NotFound:
            time.sleep(1)

    # Initialize stats tracking
    model_stats = defaultdict(
        lambda: {"duration": 0, "max_cpu_percent": 0, "max_memory_usage": 0}
    )
    current_model = None
    model_start_time = None

    for line in pipeline_container.logs(stream=True, follow=True):
        line = line.decode("utf-8").strip()

        if "inserted" in line and "objects into" in line:
            model_name = line.split("objects into")[-1].strip()

            if current_model:
                stats = capture_stats(pipeline_container, model_start_time)
                if stats:
                    model_stats[current_model]["duration"] += stats["duration"]
                    model_stats[current_model]["max_cpu_percent"] = max(
                        model_stats[current_model]["max_cpu_percent"],
                        stats["max_cpu_percent"],
                    )
                    model_stats[current_model]["max_memory_usage"] = max(
                        model_stats[current_model]["max_memory_usage"],
                        stats["max_memory_usage"],
                    )

            current_model = model_name
            model_start_time = time.time()

        if "✅ crates" in line:
            break

    end_time = time.time()
    total_runtime = end_time - start_time

    # Prepare the report
    report = {"total_runtime": total_runtime, "model_stats": dict(model_stats)}

    print("report:")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    monitor_pipeline()
