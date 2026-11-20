import time

from prefect import serve


deployments = [
    # my_flow.to_deployment(name="daily", cron="0 6 * * *"),
]


if __name__ == "__main__":
    if deployments:
        serve(*deployments)
    else:
        print("no prefect deployments configured")
        while True:
            time.sleep(3600)
