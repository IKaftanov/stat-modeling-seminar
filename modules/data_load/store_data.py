import asyncio
import os
import json
import time
import logging
import logging.config
import yaml
from aiofile import AIOFile
from fas_models import Case, Document


import win32file
win32file._setmaxstdio(2048)


ROOT_DIR = "detail_data"
DOCS_DIR = "documents"
ENTITIES_DIR = "data"


def dump_properties(_object):
    return dict((name, getattr(_object, name)) for name in dir(_object) if not name.startswith('_'))


def get_documents_from_case(case_id):
    return list(filter(lambda x: case_id in x, os.listdir("documents")))


async def get_documents(number, file_name):
    async with sem:
        try:
            async with AIOFile(f"{ROOT_DIR}\\{file_name}", "r", encoding="utf-8") as fd:
                data = await fd.read()
                case = Case(_id=file_name.split(".")[0], html_document=data)
                entity = dump_properties(case)
                for i, doc in enumerate(get_documents_from_case(case.id)):
                    async with AIOFile(f"{DOCS_DIR}\\{doc}", "r", encoding="utf-8") as fd2:
                        data2 = await fd2.read()
                        entity = {**entity, **dump_properties(Document(case_id=case.id, html_document=data2))}
                async with AIOFile(f"{ENTITIES_DIR}\\{case.id}.json", "w", encoding="utf-8") as fd3:
                    await fd3.write(json.dumps(entity, indent=4))
                logging.info(f"Finish [{number}] - {case.id}")
        except Exception as error:
            logging.critical(error)


def run():
    bd = set(os.listdir(ROOT_DIR))
    cd = set(os.listdir(ENTITIES_DIR))
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
    sem = asyncio.Semaphore(10)
    start_time = time.time()
    run()
    duration = time.time() - start_time
    logging.info(f"Load pages completed. Time: {duration}")