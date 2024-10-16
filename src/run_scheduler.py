import schedule
import time
import subprocess
import sys
import os


def run_pipeline():
    print("Running pipeline...")
    result = subprocess.run(
        [sys.executable, "/src/pipeline/main.py", "crates"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr, file=sys.stderr)


def main():
    # make sure we're in the correct directory
    os.chdir("/src")

    # debug some useful info
    print(f"Current working directory: {os.getcwd()}")
    print(f"Contents of current directory: {os.listdir('.')}")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")

    # schedule the job
    schedule.every(24).hours.do(run_pipeline)

    # run the job once immediately
    run_pipeline()

    # keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
