# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from WebSpider.YellowPages.webspider import YellowPages
from WebSpider.YellowPages.DeepExtracting import extractIndsideInformation


def generate_combined_json(json1, json2):
    return json1.dump(json2)


def extract_pages(num, term, location, state, class_):
    jsons = None
    for i in range(num+1):
        yellow_pages = class_(term, location, state, i)
        json_data = yellow_pages.generate_json()
        if jsons is None:
            jsons = json_data
        jsons = generate_combined_json(jsons, json_data)
    return jsons

def check_duplicate():
    pass


yellow_pages = YellowPages("piercing", "Philadelphia", "PA", 1)
# yellow_pages2 = YellowPages("piercing", "Philadelphia", "PA", 2)
# yellow_pages3 = YellowPages("piercing", "Philadelphia", "PA", 3)
# names = yellow_pages.extract_name_element()
# names2 = yellow_pages2.extract_name_element()
# names3 = yellow_pages3.extract_name_element()
#
#
# total_name = []
# total_name.extend(names)
# total_name.extend(names2)
# total_name.extend(names3)
# # print(total_name)
# yellow_pages4 = YellowPages("Tattos", "Philadelphia", "PA", 1)
# names4 = yellow_pages4.extract_name_element()
# address = yellow_pages4.extract_address_element()[1]
# combs = list(zip(names4, address))
# for comb in combs:
#     if comb[0] not in total_name and "Philadelphia, PA" in comb[1]:
#         pass
#         # print(comb[0], comb[1])
#
deep_info = extractIndsideInformation("piercing", "Philadelphia", "PA", 3)
emails = deep_info.extract_email_element()
# Press the green button in the gutter to run the script.

if __name__ == '__main__':
    # web = yellow_pages.extract_name_element()
    # print(web)
    # jsons = yellow_pages.generate_json()
    # print(jsons)


    print(len(emails))
    for email in emails:
        print(email)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
