from WebSpider.YellowPages.webspider import YellowPages
import requests
from bs4 import BeautifulSoup

class extractIndsideInformation(YellowPages):
    def __init__(self, term, location, state, page=1):
        super().__init__(term, location, state, page)
        self.url = self.generate_url()
        self.result_class = "result"
        self.result_tag = "div"
        self.original = "https://www.yellowpages.com"
        self.business_info = self.extract_business_info()

    def extract_business_info(self):
        business_infos = []
        links = self.extract_link_element()
        for url in links:
            if url != "No Href Found" and url != "No Business Name Found":
                res = requests.get(url)
                if res.status_code == 200:
                    results = BeautifulSoup(res.content, "html.parser")
                    result = results.find("div", id="content-container")
                    if result:
                        main_content = result.find("div", id="main-content")
                        if main_content:
                            business_info = main_content.find("section", id="business-info")
                            if business_info:
                                business_infos.append(business_info)
                            else:
                                business_infos.append("No Business Infor Found")
                        else:
                            business_infos.append("No Main Content Found")
                    else:
                        business_infos.append("No Content Container Found")
                else:
                    business_infos.append("No HTML Attached")
            else:
                business_infos.append("No Url Found")
        return business_infos

    def extract_link_element(self):
        links = []
        results = self.extract_result_elements()
        for result in results:
            business_name = result.find("a", class_="business-name")
            if business_name:
                href = business_name.get("href")
                if href:
                    links.append(self.original + href)
                else:
                    links.append("No Href Found")
            else:
                links.append("No Business Name Found")
        return links


    def extract_email_element(self):
        emails = []
        business_infos = self.extract_business_info()
        for business_info in business_infos:
            if not isinstance(business_info, str):
                email_business = business_info.find("a", class_="email-business")
                if email_business:
                    href = email_business.get("href").replace("mailto:", "")
                    emails.append(href)
                else:
                    emails.append("No Email Found")
            else:
                emails.append(business_info)
        return emails






