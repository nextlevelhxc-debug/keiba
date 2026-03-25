import os
import sys
import logging
import time
from datetime import date, datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# 上位ディレクトリのモジュールをインポートするためのパス追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_engine, get_session, init_db, Race, Horse, Result

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataIngester:
    """
    データ取得モジュール（Mac向け代替版：Netkeibaスクレイピング）。
    Webサイトへの負荷を考慮し、アクセス間隔（time.sleep）を必ず空けるようにしています。
    """
    
    def __init__(self, db_session):
        self.session = db_session
        logger.info("DataIngester が初期化されました。")

    def scrape_race_result(self, race_id: str):
        """
        特定のレースIDからレース情報と結果をスクレイピングし、DBに保存する。
        """
        url = f"https://db.netkeiba.com/race/{race_id}/"
        logger.info(f"URL: {url} からデータを取得しています...")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
            response = requests.get(url, headers=headers)
            response.encoding = 'EUC-JP'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- 1. レース情報の取得 ---
            data_intro = soup.find('div', class_='data_intro')
            if not data_intro:
                logger.error(f"レース情報が見つかりません: {race_id}")
                return
            
            race_name = data_intro.find('h1').text.strip()
            race_data = data_intro.find('p', class_='smalltxt').text.strip().split(' ')
            date_str = race_data[0]
            place = race_data[1].replace('回', '').replace('日目', '')
            # 漢字以外の場所名を抽出 (例: 2回中山4日目 -> 中山)
            place = re.sub(r'\d+', '', place)
            
            diary_info = data_intro.find('p', attrs={'class': None}).text.strip()
            # 例: 芝右2000m / 天候 : 晴 / 芝 : 良 / 発走 : 15:45
            course_type = "芝" if "芝" in diary_info else "ダート"
            distance = int(re.search(r'(\d+)m', diary_info).group(1)) if re.search(r'(\d+)m', diary_info) else 2000
            weather = re.search(r'天候 : (\w+)', diary_info).group(1) if "天候" in diary_info else "晴"
            track_condition = re.search(r' : (\w+)', diary_info.split('/')[-2]).group(1) if " : " in diary_info else "良"

            # レースDB保存
            race = self.session.query(Race).filter_by(id=race_id).first()
            if not race:
                race = Race(
                    id=race_id,
                    date=datetime.strptime(date_str, '%Y年%m月%d日').date(),
                    place=place,
                    race_number=int(race_id[-2:]),
                    race_name=race_name,
                    course_type=course_type,
                    distance=distance,
                    weather=weather,
                    track_condition=track_condition
                )
                self.session.add(race)

            # --- 2. レース結果テーブルの取得 ---
            from io import StringIO
            table = soup.find('table', class_='race_table_01')
            if not table:
                logger.error("結果テーブルが見つかりません")
                return

            df = pd.read_html(StringIO(str(table)))[0]
            
            # 列名の空白を削除 (例: '着 順' -> '着順')
            df.columns = df.columns.astype(str).str.replace(' ', '').str.replace('　', '')
            
            # タイムを秒に変換する関数
            def time_to_seconds(t_str):
                if pd.isna(t_str) or not isinstance(t_str, str) or ':' not in t_str:
                    return None
                m, s = t_str.split(':')
                return int(m) * 60 + float(s)

            # 馬IDとデータの保存
            horse_links = table.find_all('a', href=re.compile(r'/horse/\d+'))
            for i, row in df.iterrows():
                h_name = row.get('馬名')
                h_id = re.search(r'/horse/(\d+)', horse_links[i]['href']).group(1) if i < len(horse_links) else None
                
                if not h_id or pd.isna(h_name): continue

                # 馬DB保存
                horse = self.session.query(Horse).filter_by(id=h_id).first()
                if not horse:
                    horse = Horse(id=h_id, name=h_name)
                    self.session.add(horse)

                # 結果DB保存 (UPSERT)
                result = self.session.query(Result).filter_by(race_id=race_id, horse_id=h_id).first()
                if not result:
                    result = Result(race_id=race_id, horse_id=h_id)
                    
                try:
                    rank_str = str(row.get('着順', ''))
                    result.rank = int(rank_str) if rank_str.isdigit() else 99
                    
                    horse_num_str = str(row.get('馬番', '0'))
                    result.horse_number = int(horse_num_str) if horse_num_str.isdigit() else 0
                    
                    result.jockey = str(row.get('騎手', ''))
                    
                    weight_str = str(row.get('斤量', '0.0'))
                    result.weight_carried = float(weight_str) if weight_str.replace('.','').isdigit() else 0.0
                    
                    result.time_seconds = time_to_seconds(row.get('タイム', ''))
                    
                    odds_str = str(row.get('単勝', '0.0'))
                    result.odds = float(odds_str) if odds_str.replace('.','').isdigit() else 0.0
                    
                    pop_str = str(row.get('人気', '0'))
                    result.popularity = int(pop_str) if pop_str.isdigit() else 0
                    
                    f3_str = str(row.get('上り', '0.0'))
                    result.last_3f = float(f3_str) if f3_str.replace('.','').isdigit() else 0.0
                    
                    if not result.id:
                        self.session.add(result)
                except Exception as e:
                    logger.warning(f"結果のパース中にエラー (馬:{h_name}): {e}")

            self.session.commit()
            logger.info(f"レース '{race_name}' ({race_id}) の結果を{len(df)}頭分保存しました。")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"スクレイピング中にエラーが発生しました: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def fetch_past_races(self, year: int, month: int, limit: int = 10):
        """
        指定された年月のレース結果をまとめて取得する。
        """
        # 月間開催一覧URL (例: https://db.netkeiba.com/race/list/202401/)
        url = f"https://db.netkeiba.com/race/list/{year}{month:02d}/"
        logger.info(f"月間レース一覧を取得中: {url}")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.encoding = 'EUC-JP'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # レースIDの抽出 (/race/202401010101/)
        race_ids = []
        links = soup.find_all('a', href=re.compile(r'/race/\d+/'))
        for a in links:
            r_id = re.search(r'/race/(\d+)/', a['href']).group(1)
            if r_id not in race_ids:
                race_ids.append(r_id)
        
        logger.info(f"{year}年{month}月のレースを {len(race_ids)} 件発見しました。")
        
        # 取得実行
        count = 0
        for r_id in race_ids:
            if count >= limit: break
            
            # すでにデータがあるかチェック
            if self.session.query(Race).filter_by(id=r_id).first():
                logger.info(f"レースID {r_id} は既に存在するためスキップします。")
                continue

            self.scrape_race_result(r_id)
            count += 1
            time.sleep(1.5) # 負荷軽減
            
        logger.info(f"{year}年{month}月のデータを {count} 件インポート完了しました。")

if __name__ == "__main__":
    engine = get_engine()
    init_db(engine) # テーブル作成確認
    session = get_session(engine)
    
    ingester = DataIngester(session)
    # テストとして2024年1月のレースを5件取得してみる
    ingester.fetch_past_races(2024, 1, limit=5)
    
    session.close()
