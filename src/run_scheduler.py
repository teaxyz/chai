import schedule
import time
import subprocess
import sys
import os

PKG_MANAGER = os.getenv("PKG_MANAGER", "crates")
FREQUENCY = int(os.getenv("FREQUENCY", 24))


def run_pipeline():
    # using Popen so we can continuously capture output
    process = subprocess.Popen(
        [sys.executable, "/src/pipeline/main.py", PKG_MANAGER],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        universal_newlines=True,
        text=True,
    )
    # this is hacky, but ensures we capture all output
    while True:
        output = process.stdout.readline()
        if output == "" and process.poll() is not None:
            break
        if output:
            print(output.strip())
    rc = process.poll()
    if rc != 0:
        print(process.stderr.read(), file=sys.stderr)
        raise Exception(f"Pipeline failed with return code {rc}")


def main():
    # make sure we're in the correct directory
    os.chdir("/src")

    # schedule
    print(f"scheduling pipeline to run every {FREQUENCY} hours...")
    schedule.every(FREQUENCY).hours.do(run_pipeline)

    # run now
    run_pipeline()

    # keep running
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
