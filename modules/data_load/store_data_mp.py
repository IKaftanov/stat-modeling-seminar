import asyncio
import json
import os
import time
from multiprocessing import Pool

from fas_models import Case, Document

ROOT_DIR = "detail_data"
DOCS_DIR = "documents"
ENTITIES_DIR = "data"


def dump_properties(_object):
    return dict((name, getattr(_object, name)) for name in dir(_object) if not name.startswith('_'))


def get_documents_from_case(case_id):
    return list(filter(lambda x: case_id in x, os.listdir(DOCS_DIR)))


def process_document(number, total_count, file_name):
    try:
        with open(f"{ROOT_DIR}\\{file_name}", "r", encoding="utf-8") as fd:
            data = fd.read()
            case = Case(_id=file_name.split(".")[0], html_document=data)
            entity = dump_properties(case)
            documents = []
            for i, doc in enumerate(get_documents_from_case(case.id)):
                with open(f"{DOCS_DIR}\\{doc}", "r", encoding="utf-8") as fd2:
                    data2 = fd2.read()
                    documents.append(dump_properties(Document(case_id=case.id, html_document=data2)))
            entity["documents"] = documents
            with open(f"{ENTITIES_DIR}\\{case.id}.json", "w", encoding="utf-8") as fd3:
                fd3.write(json.dumps(entity, indent=4))
            print(f"Finish [{number}/{total_count}] - {case.id}")
    except Exception as error:
        print(error)


def run():
    bd = set(os.listdir(ROOT_DIR))
    cd = set([f"{item.split('.')[0]}.html" for item in os.listdir(ENTITIES_DIR)])
    print(f"Skipping: {len(cd)}")
    files = bd - cd
    total = len(files)
    print(f"Starting... Total count: {total}")
    with Pool() as pool:
        pool.starmap(process_document, [(i, total, item) for i, item in enumerate(files)])


if __name__ == '__main__':
    sem = asyncio.Semaphore(10)
    start_time = time.time()
    run()
    duration = time.time() - start_time
    print(f"Load pages completed. Time: {duration}")