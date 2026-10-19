"""Submit the local market job once the Flink session cluster is ready."""

import json
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen


FLINK_URL = "http://jobmanager:8081"
JOB_PREFIX = "eve-market-alerts-"
ACTIVE_STATES = {"CREATED", "INITIALIZING", "SCHEDULED", "DEPLOYING", "RUNNING", "RESTARTING", "RECONCILING", "FAILING", "CANCELLING"}


def get_json(path: str) -> dict:
    with urlopen(FLINK_URL + path, timeout=5) as response:
        return json.load(response)


def main() -> None:
    for attempt in range(90):
        try:
            if get_json("/overview").get("taskmanagers", 0) < 1:
                raise URLError("Waiting for a TaskManager")
            jobs = get_json("/jobs/overview").get("jobs", [])
            break
        except (OSError, URLError, ValueError) as error:
            if attempt == 89:
                raise RuntimeError("Flink did not become ready within three minutes") from error
            time.sleep(2)

    active = [job for job in jobs if job.get("name", "").startswith(JOB_PREFIX) and job.get("state") in ACTIVE_STATES]
    if active:
        print(f"Market job already active: {active[0]['jid']} ({active[0]['state']})", flush=True)
        return

    subprocess.run(["flink", "run", "-d", "-py", "/opt/processor/run_job.py"], check=True)


if __name__ == "__main__":
    main()
