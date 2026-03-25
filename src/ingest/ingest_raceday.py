import os
import sys
import logging
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup

# 上位ディレクトリのモジュールをインポートするためのパス追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_engine, get_session
try:
    from ingest_jravan import DataIngester
except ImportError:
    from .ingest_jravan import DataIngester

# ログの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RaceDayIngester(DataIngester):
    """
    レース当日に実行する「差分更新」専用のクラス。
    最新の出馬表、オッズ、馬場状態等を取得します。
    """
    
    def fetch_today_entries(self, race_id: str):
        """
        特定のレースIDの出馬表を取得するメソッド。
        """
        url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
        logger.info(f"出馬表URL: {url} からデータを取得しています...")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.encoding = 'EUC-JP'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.select_one('table.Shutuba_Table')
        
        if not table:
            logger.error("出馬表テーブルが見つかりませんでした。")
            return pd.DataFrame()
            
        from io import StringIO
        # html5lib を使用してパース
        df = pd.read_html(StringIO(str(table)), flavor='html5lib')[0]
        
        # 馬IDの抽出
        horse_ids = []
        horse_tds = table.select('td.HorseInfo')
        if not horse_tds:
            # カラム名で探す
            horse_tds = table.select('td:nth-of-type(4)') # 通常4列目
        
        for td in horse_tds:
            a_tag = td.find('a', href=re.compile(r'/horse/\d+'))
            if a_tag:
                h_id = re.search(r'/horse/(\d+)', a_tag['href']).group(1)
                horse_ids.append(h_id)
            else:
                horse_ids.append(None)
        
        # カラム名の整理
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        df.columns = [str(c).replace(' ', '').replace('\n', '') for c in df.columns]

        if len(horse_ids) == len(df):
            df['horse_id'] = horse_ids
            
        logger.info(f"{len(df)}頭の出走馬情報を取得しました（馬ID含む）。")
        return df

    def fetch_today_odds(self, race_id: str):
        """
        特定のレースの現在のオッズを取得されるメソッド。
        """
        # 単複オッズのみに絞る
        url = f"https://race.netkeiba.com/race/odds.html?type=b1&race_id={race_id}"
        logger.info(f"オッズURL: {url} から最新データを取得します...")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.encoding = 'EUC-JP'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Netkeibaのオッズテーブルは動的に生成される場合があるが、
        # サーバー側でHTMLに含まれている場合は Odds_Table クラスを持つ
        table = soup.select_one('table.Odds_Table')
        
        if not table:
            # 代替案: div.Odds_List_Table 内の table を探す
            wrapper = soup.select_one('.Odds_List_Table')
            if wrapper:
                table = wrapper.find('table')
        
        if not table:
            # 最終手段: '単勝' 期待値が含まれるテキストを持つテーブルを探す
            for t in soup.find_all('table'):
                if '単勝' in t.text and '人気' in t.text:
                    table = t
                    break
        
        if not table:
            logger.error("オッズ情報を合むテーブルが見つかりませんでした。")
            return pd.DataFrame()
            
        from io import StringIO
        try:
            dfs = pd.read_html(StringIO(str(table)), flavor='html5lib')
            df_odds = dfs[0]
            
            # カラム名をクリーニング
            if isinstance(df_odds.columns, pd.MultiIndex):
                df_odds.columns = df_odds.columns.get_level_values(-1)
            df_odds.columns = [str(c).replace(' ', '').replace('\n', '') for c in df_odds.columns]
            
            logger.info("オッズデータのパース完了")
            return df_odds
        except Exception as e:
            logger.error(f"オッズテーブルのパースに失敗しました: {e}")
            return pd.DataFrame()

def main():
    """
    データ取得のテスト用メイン関数。
    """
    logger.info("=== 当日データ差分取得テストを開始します ===")
    
    engine = get_engine()
    session = get_session(engine)
    
    try:
        raceday_ingester = RaceDayIngester(session)
        # テスト用レースID (2026年3月8日 弥生賞)
        test_race_id = "202606020411"
        
        entries = raceday_ingester.fetch_today_entries(test_race_id)
        odds = raceday_ingester.fetch_today_odds(test_race_id)
        
        if not entries.empty:
            logger.info("--- 出馬表サンプール ---")
            print(entries.columns)
            print(entries.head())
        if not odds.empty:
            logger.info("--- オッズサンプル ---")
            print(odds.columns)
            print(odds.head())
            
    finally:
        session.close()
        logger.info("=== テスト終了 ===")

if __name__ == "__main__":
    main()
