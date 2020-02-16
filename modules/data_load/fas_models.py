from bs4 import BeautifulSoup
import re
import datetime
import uuid


def _get_text_from_tag(item):
    element = item.find("a")
    if not element:
        element = item.find("span")
    return re.sub(r"\s+", ' ', element.text).strip() if element and element.text else None


def _convert_timestamp(date):
    if date:
        try:
            date = datetime.datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d")
        except ValueError:
            date = None
    return date


class Case:
    def __init__(self, _id, html_document):
        self.id = _id
        self.__soup = BeautifulSoup(html_document, "html.parser")
        self.__description_block = self.__soup.find("div", {"style": "background-color: #f6f6f6; padding: 1rem 0; margin-top: 1rem;"})
        self.__description_items = self.__description_block.find_all_next("div", {"class": "col-sm-6"})

    @property
    def title(self):
        title = self.__soup.find_all("h3")[1].text
        return title if title else None

    @property
    def procedure(self):
        return _get_text_from_tag(self.__description_items[0])

    @property
    def department(self):
        return _get_text_from_tag(self.__description_items[2])

    @property
    def sector(self):
        return _get_text_from_tag(self.__description_items[3])

    @property
    def stage(self):
        return _get_text_from_tag(self.__description_items[5])

    @property
    def start_date(self):
        return _convert_timestamp(_get_text_from_tag(self.__description_items[4]))

    @property
    def close_date(self):
        return _convert_timestamp(_get_text_from_tag(self.__description_items[6]))

    @property
    def registration_date(self):
        return _convert_timestamp(_get_text_from_tag(self.__description_items[1]))

    @property
    def linked_documents(self):
        documents = []
        documents_tags = self.__soup.find_all("div", {"style": "padding: 0 0 1rem;"})
        documents_names = self.__soup.find_all("div", {"style": "font-weight: 600; font-size: 14pt; padding: 0 0 1rem;"})
        if documents_tags and documents_names:
            for tag, name in zip(documents_tags, documents_names):
                if tag and name and name.text:
                    if "решение" in name.text.lower():
                        link = tag.find("a", href=True)
                        if link and link["href"]:
                            documents.append(link["href"].split("/")[-2])
        return documents


class Document:
    def __init__(self, case_id, html_document):
        self.case_id = case_id
        self.__soup = BeautifulSoup(html_document, "html.parser")
        self.__description_block = self.__soup.find("div", {"style": "background-color: #f6f6f6; padding: 1rem 0; margin-top: 1rem;"})
        self.__description_items = self.__description_block.find_all_next("div", {"class": "col-sm-6"})

    @property
    def id(self):
        pdf_button = self.__soup.find("a", {"class": "btn btn-link"})
        if pdf_button and pdf_button["href"]:
            return pdf_button["href"].split('/')[-2]
        else:
            return str(uuid.uuid4())

    @property
    def title(self):
        title = self.__soup.find_all("h3")[1].text
        return title if title else None

    @property
    def type(self):
        return _get_text_from_tag(self.__description_items[0])

    @property
    def registration_date(self):
        return _convert_timestamp(_get_text_from_tag(self.__description_items[1]))

    @property
    def text(self):
        raw_text = ""
        full_text_element = self.__soup.find("div", {"id": "document_text_container"})
        if full_text_element:
            raw_text = "".join([item.text for item in full_text_element.find_all_next("p")])
            return re.sub(r"\s+", ' ', raw_text).strip()
        return raw_text
