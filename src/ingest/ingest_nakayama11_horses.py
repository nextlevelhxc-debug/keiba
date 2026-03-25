import sys
import os
import logging
import requests
from bs4 import BeautifulSoup
import re
import time

# パス追加
sys.path.append(os.path.abspath('src'))
from database import get_engine, get_session
from ingest.ingest_jravan import DataIngester
from analysis.horse_scoring import HorseScorer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_past_races_for_horse(horse_id: str, ingester: DataIngester, scorer: HorseScorer):
    url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
    logger.info(f"馬の過去成績を取得中: {url}")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    response = requests.get(url, headers=headers)
    response.encoding = 'EUC-JP'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    race_ids = []
    # 過去成績テーブルからレースIDを探す
    table = soup.find('table', class_='db_h_race_results')
    if table:
        links = table.find_all('a', href=re.compile(r'/race/\d+'))
        for a in links:
            match = re.search(r'/race/(\d+)', a['href'])
            if match:
                r_id = match.group(1)
                # '/race/202606010207/' 形式に一致し、かつ映画/画像へのリンク(movie等)でないか確認
                if r_id.isdigit() and len(r_id) == 12 and r_id not in race_ids:
                    race_ids.append(r_id)
    
    logger.info(f"馬ID {horse_id} の過去レースを {len(race_ids)} 件発見しました。")
    
    for count, r_id in enumerate(race_ids):
        # スコア計算のため、最新の10走を取得する
        if count >= 10: break
        
        # すでに取得済みならDBにあるが、ingester内でチェックしているのでそのまま呼ぶ
        ingester.scrape_race_result(r_id)
        time.sleep(1.0)
        
    # スコア更新
    scorer.calculate_and_update(horse_id)

if __name__ == "__main__":
    engine = get_engine()
    session = get_session(engine)
    ingester = DataIngester(session)
    scorer = HorseScorer(session)
    
    # 中山11R (弥生賞) の馬ID
    nakayama_11r_horses = [
        "2023104346", "2023103986", "2023106492", "2023103687", "2023106861",
        "2023105311", "2023105433", "2023107247", "2023103814", "2023105354"
    ]
    
    for h_id in nakayama_11r_horses:
        fetch_past_races_for_horse(h_id, ingester, scorer)
        time.sleep(1.0)
        
    session.close()
    logger.info("全馬の過去成績取得とスコア更新が完了しました。")
