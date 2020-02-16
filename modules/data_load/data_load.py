import asyncio
import logging
import logging.config
import os
import time
from random import randint

import aiohttp
import yaml

from fas_gov import Pages


def get_pages():
    urls = []
    for i in range(32000):
        urls.append(f"http://br.fas.gov.ru/?page={i}")
    return urls


def get_details_pages():
    data = []
    for file in os.listdir("pages"):
        with open(f"pages\\{file}", "r", encoding="utf-8") as fd:
            data += Pages(fd.read()).links()
    return [f"http://br.fas.gov.ru{item}" for item in data]


async def load_pages(url, number, destination, _type):
    async with sem:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    text = await response.text()
                    if response.status == 200:
                        fn = f"{destination}\\{url.split('=')[-1]}.html" if _type == "page" else f"{destination}\\{url.split('/')[-2]}.html"
                        with open(fn, "w", encoding="utf-8") as fd:
                            fd.write(text)
                    sleep_time = randint(20, 60) / 10
                    logging.info(f"Url[{number}]: {url}: {response.status}: {sleep_time}")
                    await asyncio.sleep(sleep_time)
        except Exception as error:
            logging.critical(error)


def run(urls, func, destination, _type):
    routines = [func(item, i, destination, _type) for i, item in enumerate(urls)]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*routines))


if __name__ == "__main__":
    with open("logger.yaml", 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

    sem = asyncio.Semaphore(30)
    # load pages
    start_time = time.time()
    # run(pages, load_pages, "pages", "page")
    duration = time.time() - start_time
    logging.info(f"Load pages completed. Time: {duration}")

    # load details
    detail_pages = get_details_pages()
    logging.info(len(detail_pages))
    start_time = time.time()
    # run(detail_pages, load_pages, "detail_data", "detail")
    duration = time.time() - start_time
    logging.info(f"Load pages completed. Time: {duration}")
