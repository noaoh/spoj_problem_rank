from bs4 import BeautifulSoup
from collections import defaultdict


def parse_rank_page(page):
    soup = BeautifulSoup(page, "html.parser")
    # the second table is the table with all the user data
    table_body = soup.find_all("tbody")[1]
    # the columns are in this order
    header = ["rank", "date", "user", "result", "time", "memory", "language"]
    data = defaultdict(list)
    data[0] = header
    # parse data in rows 
    rows = table_body.find_all("tr")
    for row in rows:
        row_data = [e.text.strip() for e in row.find_all("td")]
        rank = row_data[0]
        data[rank] = row_data

    return data 


def parse_user_page(page):
    soup = BeautifulSoup(page, "html.parser")
    user_data = soup.find("div", attrs={"id": "user-profile-left"})
    if user_data is None:
        return {}

    username = user_data.h4.text[1:].strip()
    ps = user_data.find_all("p")
    if len(ps) < 4:
        return {}

    institution = ps[3].text.replace("Institution:", "").strip()

    return {username: institution}

