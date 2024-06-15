import requests
from bs4 import BeautifulSoup

URL = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"

def extract_data(url):
    res = requests.get(url, timeout=10)
    
    print(res.text)
    return res.text


def trans_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    table= soup.find_all('table')[2]
    print(table)
    table_rows = table.find_all('tr')
    
    return table_rows
def print_data():
    html = extract_data(URL)
    rows = trans_data(html)
    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        value = {
            'rank': i,
            'stadium': tds[0].text, 
            'coffe': tds[1].text,
            'coffee': tds[2].text,
            'coffee': tds[4].text,
        }
        data.append(value)
    print(len(rows))
    data.append(value)
    print(data)

print_data()