import datetime
import re
import uuid

from bs4 import BeautifulSoup


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


class Page:
    def __init__(self, html_document):
        self.__soup = BeautifulSoup(html_document, "html.parser")

    def _card(self, card_object):
        rows = card_object.find_all("div", {"class": "row"})
        head = rows[0].find("a", href=True)
        title_href, title = head["href"], head.text
        date = rows[0].find("div", {"class": "col-sm-2 text-right"}).text
        date = re.findall(r"(\d{2}\.\d{2}\.\d{4})", date)[0] if date else None

        target = rows[1].find("a", href=True)
        target_href, target_name = target["href"], target.text
        if len(rows) > 2:
            tags = []
            for tag in rows[2].find_all("a", {"class": "badge badge-disabled"}):
                tag_name = tag.text.strip("\n").strip(" ")
                tags.append({"tag_href": tag["href"], "tag_name": tag_name})

            department = rows[2].find("div", {"class": "col-sm-3 text-muted text-right"})
            if department:
                department = department.find("a")
                department_href, department_name = department["href"], department.text
            else:
                department_href, department_name = None, None
        else:
            tags = []
            department_href, department_name = None, None

        model = {
            "title_href": title_href,
            "title": title,
            "date": date,
            "target_href": target_href,
            "target_name": target_name,
            "tags": tags,
            "department_href": department_href,
            "department_name": department_name
        }

        return model

    @property
    def items(self):
        elements = self.soup.find_all("div", {"class": "grey-card"})
        data = []
        for element in elements:
            data.append(self._card(element))
        return data

    @property
    def links(self):
        elements = self.__soup.find_all("div", {"class": "grey-card"})
        data = []
        for element in elements:
            rows = element.find_all("div", {"class": "row"})
            head = rows[0].find("a", href=True)
            title_href = head["href"]
            data.append(title_href)
        return data


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
