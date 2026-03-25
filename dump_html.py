import requests

url = "https://db.netkeiba.com/horse/2023104346/"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
response = requests.get(url, headers=headers)
response.encoding = 'EUC-JP'
with open("/tmp/horse.html", "w", encoding="utf-8") as f:
    f.write(response.text)
