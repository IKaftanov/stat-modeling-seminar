import re

from bs4 import BeautifulSoup


class Pages:
    def __init__(self, html_page):
        self.html_page = html_page
        self.soup = BeautifulSoup(self.html_page, "html.parser")

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

    def items(self):
        elements = self.soup.find_all("div", {"class": "grey-card"})
        data = []
        for element in elements:
            data.append(self._card(element))
        return data

    def links(self):
        elements = self.soup.find_all("div", {"class": "grey-card"})
        data = []
        for element in elements:
            rows = element.find_all("div", {"class": "row"})
            head = rows[0].find("a", href=True)
            title_href = head["href"]
            data.append(title_href)
        return data


class DetailView:
    def __init__(self, html_page):
        self.html_page = html_page
        self.soup = BeautifulSoup(self.html_page, "html.parser")

    def base_info(self):
        base = self.soup.find("div", {"style": "background-color: #f6f6f6; padding: 1rem 0; margin-top: 1rem;"})
        model = {}
        for item in base.find_all("div", {"class": "col-sm-6"}):
            name = item.find("div", {"class": "col-sm-4 text-right"}).text
            value_tag = item.find("div", {"class": "col-sm-8"})
            if value_tag.find("a"):
                value = value_tag.find("a").text
            elif value_tag.find("span"):
                value = value_tag.find("span").text
            else:
                value = None
            model[name] = value
        return model

    def docs(self):
        docs = []
        docs_tags = self.soup.find_all("div", {"style": "padding: 0 0 1rem;"})
        docs_names = self.soup.find_all("div", {"style": "font-weight: 600; font-size: 14pt; padding: 0 0 1rem;"})
        if docs_tags and docs_names:
            for tag, name in zip(docs_tags, docs_names):
                if tag and name and name.text:
                    if "решение" in name.text.lower():
                        link = tag.find("a")
                        if link and link["href"]:
                            docs.append(link["href"])
        return docs


class Document:
    def __init__(self, html_document):
        self.html_document = html_document
        self.soup = BeautifulSoup(self.html_document, "html.parser")

    def text(self):
        full_text_element = self.soup.find("div", {"id": "document_text_container"})
        raw_text = "".join([item.text for item in full_text_element.find_all_next("p")])
        return raw_text
