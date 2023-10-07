import datetime
import requests
from bs4 import BeautifulSoup
import json

class YellowPages:
    def __init__(self, term, location, state, page=1):
        self.term = term
        self.location = location
        self.state = state
        self.page = page
        self.url = self.generate_url()
        self.result_class = "result"
        self.result_tag = "div"
        self.results = self.extract_result_elements()
        self.original = "https://www.yellowpages.com/"

    def generate_url(self):
        if self.page == 1:
            return f"https://www.yellowpages.com/search?search_terms={self.term}&geo_location_terms=" \
                   f"{self.location}%2C%20{self.state}"
        else:
            return f"https://www.yellowpages.com/search?search_terms={self.term}&geo_location_terms=" \
                   f"{self.location}%2C%20{self.state}&page={self.page}"

    def extract_result_elements(self):
        headers = {
            "User-Agent": "Your User-Agent String Here",
            "Accept-Language": "en-US,en;q=0.5",
        }
        response = requests.get(self.url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            results = soup.find_all(self.result_tag, class_=self.result_class)
        else:
            return "Failed to request HTML"
        return results if len(results) != 0 else "No Result Found"

    def extract_name_element(self):
        elements = []
        if not isinstance(self.results, str):
            for result in self.results:
                business_name = result.find_all("a", class_="business-name")
                if business_name:
                    for shop in business_name:
                        span_element = shop.find_all("span")
                        if span_element:
                            element_text = ""
                            for element in span_element:
                                element_text += element.text.strip()
                            elements.append(element_text)
                        else:
                            elements.append("No Span Found")
                else:
                    elements.append("No Business Name Found")
        else:
            elements.append("No Result Found")
        return elements

    def extract_type_element(self):
        type_list = []
        if not isinstance(self.results, str):
            for result in self.results:
                types = result.find_all("div", class_="categories")
                if types:
                    for tp in types:
                        span_element = tp.find_all("a")
                        if span_element:
                            element_text = ""
                            for element in span_element:
                                element_text += " " + element.text.strip()
                            type_list.append(element_text)
                        else:
                            type_list.append("Unknown business type")
                else:
                    type_list.append("Unknown business type")
        else:
            type_list.append("No Result Found")
        return type_list

    # def extract_email_element(self):
    #     original = "https://www.yellowpages.com"
    #     links = []
    #     email = []
    #     results = self.extract_result_elements()
    #     for result in results:
    #         business_name = result.find_all("a", class_="business-name")
    #         if business_name:
    #             for name in business_name:
    #                 href = name.get("href")
    #                 if href:
    #                     links.append(original + href)
    #                 else:
    #                     links.append("No Href Found")
    #         else:
    #             links.append("No Business Name Found")
    #
    #     for link in links:
    #         if link != "No Href Found" and link != "No Business Name Found":
    #             pass

    def extract_website_element(self):
        websites = []
        if not isinstance(self.results, str):
            for result in self.results:
                links = result.find_all("div", class_="links")
                for link in links:
                    link_tags = link.find_all("a", class_="track-visit-website")
                    if link_tags:
                        for link_tag in link_tags:
                            data_analytics = link_tag.get("data-analytics")
                            if data_analytics:
                                analytics_data = json.loads(data_analytics)
                                href = analytics_data.get("dku")
                                if href:
                                    websites.append(href)
                                else:
                                    websites.append("No Herf Found")
                            else:
                                websites.append("No Data Analytics Tag Found")
                    else:
                        websites.append("No Link Found")
        else:
            websites.append("No Result Found")
        return websites

    def extract_phone_element(self):
        phones = []
        if not isinstance(self.results, str):
            for result in self.results:
                phone_numbers = result.find_all("div", class_="phones phone primary")
                if phone_numbers:
                    for num in phone_numbers:
                        phone = num.text.strip()
                        phones.append(phone)
                else:
                    phones.append("Phone is Unknown")
        else:
            phones.append("No Result Found")
        return phones

    def extract_address_element(self):
        streets = []
        states = []
        if not isinstance(self.results, str):
            for result in self.results:
                address = result.find_all("div", class_="street-address")
                state_postcode = result.find_all("div", class_="locality")
                if address:
                    for ad in address:
                        current_address = ad.text.strip()
                        streets.append(current_address)
                else:
                    streets.append("Address is not Found")
                if state_postcode:
                    for sp in state_postcode:
                        shop_sp = sp.text.strip()
                        states.append(shop_sp)
                else:
                    states.append("Unknown states and postcode Found")
        else:
            streets.append("No Result Found")
            states.append("No Result Found")
        return streets, states

    def extract_year_element(self):
        years = []
        if not isinstance(self.results, str):
            for result in self.results:
                numbers = result.find("div", class_="number")
                if numbers:
                    year = numbers.text.strip()
                    years.append(year)
                else:
                    years.append("Unknown")
        else:
            years.append("No Result Found")
        return years

    def extract_amenities_element(self):
        amenities_list = []
        if not isinstance(self.results, str):
            for result in self.results:
                amenities = result.find_all("div", class_="amenities")
                if amenities:
                    amenities_list.append("YES")
                else:
                    amenities_list.append("NO")
        else:
            amenities_list.append("No Result Found")
        return amenities_list

    def generate_json(self):
        data = {
            "Website": "Yellow Pages",
            "TimeStamp": str(datetime.datetime.now()),
            "items": {}
        }
        names = self.extract_name_element()
        types = self.extract_type_element()
        websites = self.extract_website_element()
        phones = self.extract_phone_element()
        street_address = self.extract_address_element()[0]
        state_and_post = self.extract_address_element()[1]
        years = self.extract_year_element()
        amenities = self.extract_amenities_element()

        for i in range(len(names)):
            business = {
                "Type": types[i],
                "Website": websites[i],
                "Phone Number": phones[i],
                "Street Address": street_address[i],
                "State and PostCode": state_and_post[i],
                "Opening Years": int(years[i]) if years[i].isdigit() else years[i],
                "Amenities Condition": amenities[i]
            }

            data["items"][names[i]] = business

        json_data = json.dumps(data, indent=2)
        jsons = json.loads(json_data)
        return jsons



