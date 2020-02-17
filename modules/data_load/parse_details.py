import asyncio
import logging.config
import os
import time
from random import randint

import requests
import yaml
from aiofile import AIOFile

from fas_models import Case

BASE_DIR = "detail_data"
CHILD_DIR = "documents"
BASE_URL = "http://br.fas.gov.ru"


async def get_documents(number, total_count, file_name):
    async with sem:
        try:
            async with AIOFile(f"{BASE_DIR}\\{file_name}", "r", encoding="utf-8") as fd:
                data = await fd.read()
                case = Case(_id=file_name, html_document=data)
                for i, doc in enumerate(case.linked_documents):
                    response = requests.get(BASE_URL + doc)
                    if response.status_code == 200:
                        with open(f"{CHILD_DIR}\\{file_name.split('.')[0]}__{i}.html", "w", encoding="utf-8") as f:
                            f.write(response.text)
                sleep_time = randint(20, 60) / 10
                logger.info(f"Filename[{number}/{total_count}]: {case.id} / {case.linked_documents} finished. Sleep for [{sleep_time}]")
                await asyncio.sleep(sleep_time)
        except Exception as error:
            logger.critical(error)


def run():
    bd = set(os.listdir(BASE_DIR))
    cd = set([f"{item.split('__')[0]}.html" for item in os.listdir(CHILD_DIR)])
    logger.info(f"Skipping: {len(cd)}")
    files = bd - cd
    total = len(files)
    logger.info(f"Starting... Total count: {total}")
    routines = [get_documents(i, total, item) for i, item in enumerate(files)]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*routines))


if __name__ == '__main__':
    with open("logger.yaml", 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    sem = asyncio.Semaphore(1000)

    start_time = time.time()
    run()
    duration = time.time() - start_time
    logger.info(f"Load pages completed. Time: {duration}")
