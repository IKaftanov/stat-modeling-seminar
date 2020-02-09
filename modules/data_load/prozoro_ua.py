import requests
import logging
from time import sleep
from random import randint
from collections import ChainMap

BASE_URL = "http://lb-api-sandbox.prozorro.gov.ua/api/2.5"

PROXIES = []


class ApiWrapper:
    __name__ = "prozorro.gov.ua-api"

    def __init__(self):
        self.in_fly_version = 0
        self.logger = logging.getLogger(self.__name__)
        self.session = requests.Session()
        self.uri = BASE_URL
        self.proxies = PROXIES

    def _change_shell(self):
        self.session.headers.update({'User-agent': f"{self.__name__}-{self.in_fly_version}"})
        self.in_fly_version += 1
        if len(self.proxies) == 0:
            raise Exception("I am done")
        self.session.proxies = self.proxies.pop(0)
        sleep(randint(15, 100) / 10)

    def _request(self, url, request_type="GET"):
        response = self.session.request(request_type, url)
        if response.status_code == 429:
            self.logger.info(f"To many requests. Changing the shell")
            self._change_shell()
        try:
            return response.json()
        except Exception as error:
            self.logger.critical(f"Json parsing error {error}. {response.status_code}:{response.content}")

    def get_tenders(self, offset=''):
        """
        Direct request for tenders page
        :param offset: page_id
        :return: dict
        """
        return self._request(f"{self.uri}/tenders?offset={offset}")

    def get_tenders_iter(self, time_sleep=1):
        """
        Iterate over pages of tenders
        :param time_sleep: time to sleep between pages
        :return: Iterable tender ids
        """
        data = self.get_tenders()
        next_page = True
        while next_page:
            for item in data['data']:
                yield item
            offset = data['next_page']['path'].split('?offset=')[-1]
            data = self.get_tenders(offset=offset)
            self.logger.info(f"offset: {offset} response: {data}")
            if offset == data['next_page']['path'].split('?offset=')[-1]:
                next_page = False
            sleep(time_sleep)

    def get_tender(self, tender_id):
        """
        Get tender by id
        :param tender_id: int
        :return: dict
        """
        return self._request(f"{self.uri}/tenders/{tender_id}")

    def tenders_batch(self, batch_size=1000, page_time_sleep=2):
        """

        :param page_time_sleep: time to wait between pages in api to avoid overload of services (I don't find anu docs)
        :param batch_size: the default page size is 100 then I am using 10 page per time
        :return: list of {'id': '6be521090fa444c881e27af026c04e8a', 'dateModified': '2019-08-14T17:46:11.742192+03:00'}
        """
        batch = []
        for item in self.get_tenders_iter(time_sleep=page_time_sleep):
            batch.append(item)
            if len(batch) > batch_size:
                yield batch
                del batch[:]
        else:
            yield batch


class DataMap:
    __name__ = 'prozorro.gov.ua-map'

    def __init__(self, tender_dict):
        self.tender_dict = tender_dict
        self.tender_id = self.tender_dict.get('id', None)
        if not self.tender_id:
            raise Exception('`tender_id` key is missing in input')

    @staticmethod
    def _unpack_nested_dict(_data, multiple_key):
        _result = []
        parent_key, _keys = multiple_key.split('_')
        for _key in _keys.split('+'):
            _result.append({f"{parent_key}_{_key}": _data[parent_key].get(_key, None) if parent_key in _data else None})
        return dict(ChainMap(*_result))

    def _unpack_nested_columns(self, _data, columns_list):
        _result = []
        for column in columns_list:
            _result.append(self._unpack_nested_dict(_data, column))
        return dict(ChainMap(*_result))

    def tender(self):
        tender_columns = ['id', 'tenderID', 'owner', 'title', 'description', 'date', 'dateModified', 'auctionUrl',
                          'status',
                          'procurementMethod', 'mainProcurementCategory', 'procurementMethodDetails',
                          'procurementMethodType', 'cause', 'causeDescription']
        tender_columns_nested = ['value_currency+amount+valueAddedTaxIncluded',
                                 'guarantee_currency+amount',
                                 'minimalStep_currency+amount+valueAddedTaxIncluded',
                                 'enquiryPeriod_startDate+endDate',
                                 'tenderPeriod_startDate+endDate',
                                 'auctionPeriod_startDate+endDate',
                                 'awardPeriod_startDate+endDate']
        base_row = {item: self.tender_dict.get(item, None) for item in tender_columns}
        full_row = {**base_row, **self._unpack_nested_columns(self.tender_dict, tender_columns_nested)}
        return full_row

    def procuring(self):
        procuring_columns = ['name', 'kind']
        procuring_columns_nested = ['contactPoint_telephone+url+name+email',
                                    'identifier_scheme+uri+legalName',
                                    'address_postalCode+countryName+streetAddress+region+locality']

        procuring = self.tender_dict.get('procuringEntity', None)

        if procuring:
            base_row = {item: procuring.get(item, None) for item in procuring_columns}
            full_row = {**base_row,
                        **self._unpack_nested_columns(procuring, procuring_columns_nested),
                        'tender_id': self.tender_id}
            return full_row
        return None

    def lots(self):
        lots_columns = ['status', 'description', 'title', 'date', 'id']
        lots_columns_nested = ['minimalStep_currency+amount+valueAddedTaxIncluded',
                               'auctionPeriod_startDate+shouldStartAfter',
                               'value_currency+amount+valueAddedTaxIncluded',
                               'guarantee_currency+amount']
        lots = []
        for item in self.tender_dict.get("lots", []):
            base_row = {lot: item.get(lot, None) for lot in lots_columns}
            full_row = {**base_row,
                        **self._unpack_nested_columns(item, lots_columns_nested),
                        'tender_id': self.tender_id}
            lots.append(full_row)
        return lots

    def complaints(self):
        complaints_columns = ['status', 'title', 'dateSubmitted', 'complaintID', 'date', 'type', 'id']
        complaints = []
        for item in self.tender_dict.get('complaints', []):
            base_row = {complaint: item.get(complaint, None) for complaint in complaints_columns}
            full_row = {'tender_id': self.tender_id, **base_row}
            complaints.append(full_row)
        return complaints

    def docs(self, tender):
        documents_columns = ['id', 'author', 'format', 'url', 'title', 'documentOf', 'datePublished', 'dateModified']
        documents = []
        for item in self.tender_dict.get('documents', []):
            base_row = {document: item.get(document, None) for document in documents_columns}
            full_row = {'tender_id': self.tender_id, **base_row}
            documents.append(full_row)
        return documents

    def author(self):
        authors_columns = ['name']
        authors_columns_nested = ['contactPoint_telephone+url+name+email', 'identifier_scheme+id+uri+legalName',
                                  'address_postalCode+countryName+streetAddress+region+locality']
        author = self.tender_dict.get('author', None)
        if author:
            base_row = {a: author.get(a, None) for a in authors_columns}
            full_row = {'tender_id': self.tender_id, **base_row,
                        **self._unpack_nested_columns(author, authors_columns_nested)}
            return full_row
        return None

    def milestones(self):
        milestones_columns = ['relatedLot', 'code', 'description', 'sequenceNumber', 'title', 'percentage', 'type',
                              'id']
        milestones_columns_nested = ['duration_type+days']
        milestones = []
        for item in self.tender_dict.get("milestones", []):
            base_row = {milestone: item.get(milestone, None) for milestone in milestones_columns}
            full_row = {
                'tender_id': self.tender_id,
                **base_row,
                **self._unpack_nested_columns(item, milestones_columns_nested)
            }
            milestones.append(full_row)
        return milestones

    def items(self):
        items_columns = ['relatedLot', 'description', 'id', 'quantity']
        items_columns_nested = ['classification_scheme+description+id',
                                'deliveryAddress_postalCode+countryName+streetAddress+region+locality',
                                'deliveryDate_startDate+endDate',
                                'unit_code+name']
        items = []
        for item in self.tender_dict.get("items", []):
            base_items_row = {i: item.get(i, None) for i in items_columns}
            full_items_row = {
                'tender_id': self.tender_id,
                **base_items_row,
                **self._unpack_nested_columns(item, items_columns_nested)
            }
            items.append(full_items_row)
        return items
