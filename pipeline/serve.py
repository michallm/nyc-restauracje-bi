import time

from prefect import serve
from prefect.schedules import Cron

from flows.complaints_311 import complaints_311


deployments = [
    complaints_311.to_deployment(
        name="daily",
        schedule=Cron("0 6 * * *", timezone="Europe/Warsaw"),
        paused=False,
    )
]


if __name__ == "__main__":
    if deployments:
        serve(*deployments)
    else:
        print("no prefect deployments configured")
        while True:
            time.sleep(3600)
