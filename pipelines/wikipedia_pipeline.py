import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from geopy import Nominatim
from datetime import datetime

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    print("Dang lay thong tin tu ", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Loi xay ra tai: {e}")


def get_wikipedia_data(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("table")[2]
    table_rows = table.find_all('tr')
    
    return table_rows

# tuy chinh thong tin
def clean_text(text):
    text = str(text).strip()
    text = text.replace('Â', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != 1:
        text = text.split('[')[0]
    if text.find("['',") != -1:
        text = text.split("['',")[1]
        text = text.replace(["', ]"], "")
    return text.replace("\n", "")

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    data = []
    for i in range(1, len(rows)):
        tds = rows[i].find_all("td")
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.',''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images':clean_text(tds[5].find('img').get("src").split("//") if tds[5].find('img') else "no image"),
            'home_team': clean_text(tds[6].text)
        }
        data.append(values)
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    
    return "Thanh cong"


def get_latitude_logtitude(country, city):
    geolocator = Nominatim(user_agent='minh_toan_test_api_geopy')
    location = geolocator.geocode(f"{city}, {country}")
    
    if location:
        return location.latitude, location.longitude

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    
    data = json.loads(data)
    stadium_df = pd.DataFrame(data)
    stadium_df['location'] = stadium_df.apply(lambda x: get_latitude_logtitude(x['country'], x['stadium']), axis=1)
    stadium_df['images'] = stadium_df['images'].apply(lambda x: x if x not in ['no image', '', None] else NO_IMAGE)
    stadium_df['capacity'] = stadium_df['capacity'].astype(int)

    duplicates = stadium_df[stadium_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_latitude_logtitude(x['country'], x['city']), axis = 1)
    stadium_df.update(duplicates)
    
    kwargs['ti'].xcom_push(key = 'rows', value = stadium_df.to_json())
    
    return 'Hoan Tat'


def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wiki_pedia_data')
    
    data = json.loads(data)
    data = pd.DataFrame(data)
    file_name = ('stadium_cleaned_data' + str(datetime.now().date())+ "_" + str(datetime.now().time()).replace(':', ',' ) + '_'+ '.csv')
    
    data.to_csv(f'data/{file_name}', index=False)