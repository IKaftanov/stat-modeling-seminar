import asyncio
import time
import os
import logging
import logging.config
import yaml

from fas_gov import DetailView
from aiofile import AIOFile
import requests
from random import randint


BASE_DIR = "detail_data"
CHILD_DIR = "documents"
BASE_URL = "http://br.fas.gov.ru"


async def get_documents(number, file_name):
    async with sem:
        try:
            async with AIOFile(f"{BASE_DIR}\\{file_name}", "r", encoding="utf-8") as fd:
                data = await fd.read()
                for i, doc in enumerate(DetailView(data).docs()):
                    response = requests.get(BASE_URL + doc)
                    if response.status_code == 200:
                        with open(f"{CHILD_DIR}\\{file_name.split('.')[0]}__{i}.html", "w", encoding="utf-8") as f:
                            f.write(response.text)
                sleep_time = randint(20, 60) / 10
                logging.info(f"Filename[{number}]: {file_name} finished. Sleep for [{sleep_time}]")
                await asyncio.sleep(sleep_time)
        except Exception as error:
            logging.critical(error)


def run():
    bd = set(os.listdir(BASE_DIR))
    cd = set([f"{item.split('__')[0]}.html" for item in os.listdir(CHILD_DIR)])
    logging.info(f"Skipping: {len(cd)}")
    files = bd - cd
    routines = [get_documents(i, item) for i, item in enumerate(files)]
    logging.info(f"Starting... Total count: {len(files)}")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*routines))


if __name__ == '__main__':
    with open("logger.yaml", 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    sem = asyncio.Semaphore(1000)

    start_time = time.time()
    run()
    duration = time.time() - start_time
    logging.info(f"Load pages completed. Time: {duration}")
