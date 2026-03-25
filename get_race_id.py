import requests
from bs4 import BeautifulSoup
import re

url = "https://race.netkeiba.com/top/race_list.html?kaisai_date=20260314"
res = requests.get(url)
res.encoding = 'EUC-JP'
soup = BeautifulSoup(res.text, 'html.parser')

for a in soup.find_all('a', href=re.compile(r'/race/shutuba.html\?race_id=')):
    href = a['href']
    match = re.search(r'race_id=(\d+)', href)
    if match:
        race_id = match.group(1)
        name_tag = a.find('span', class_='ItemTitle')
        name = name_tag.text if name_tag else ""
        print(f"Race ID: {race_id}, Name: {name}, Full text: {a.text.strip().replace('\n', '')}")
