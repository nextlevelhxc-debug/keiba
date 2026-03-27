import sys
import os
import time
import random
import logging
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_engine, get_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

class NetkeibaIngester:
    def __init__(self, session):
        self.session = session
        
    def safe_get(self, url):
        """負荷をかけないようにスリープを入れてGETリクエスト"""
        time.sleep(random.uniform(1.2, 2.5)) # 1.2秒〜2.5秒のランダムスリープ
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.raise_for_status()
            res.encoding = 'EUC-JP'
            return BeautifulSoup(res.text, 'html.parser')
        except Exception as e:
            logger.error(f"URL取得エラー {url}: {e}")
            return None

    def get_graderace_ids(self, year):
        """指定した年の重賞レース(G1, G2, G3)のIDリストをnetkeibaから取得"""
        logger.info(f"{year}年の重賞レースID一覧を検索しています...")
        url = "https://db.netkeiba.com/?pid=race_list"
        # 検索条件 (G1, G2, G3 をチェック)
        data = {
            'pid': 'race_list',
            'word': '',
            'start_year': str(year),
            'start_mon': '1',
            'end_year': str(year),
            'end_mon': '12',
            'grade[]': ['1', '2', '3'], # G1, G2, G3
            'list': '100', # 最大100件表示
            'sort': 'date'
        }
        
        race_ids = []
        page = 1
        while True:
            data['page'] = str(page)
            try:
                time.sleep(1.5)
                # POST で検索結果を取得
                res = requests.post(url, headers=HEADERS, data=data, timeout=10)
                res.raise_for_status()
                res.encoding = 'EUC-JP'
                soup = BeautifulSoup(res.text, 'html.parser')
                
                current_page_ids = []
                # 結果テーブルから /race/YYYYPPKKDDRR/ のリンクを探す
                for a in soup.select('table.race_table_01 tr td a[href^="/race/"]'):
                    href = a['href']
                    match = re.search(r'/race/(\d{12})', href)
                    if match:
                        current_page_ids.append(match.group(1))
                        
                if not current_page_ids:
                    break # これ以上結果がない
                
                race_ids.extend(current_page_ids)
                
                if page >= 10: # 安全装置
                    break
                page += 1
                
            except Exception as e:
                logger.error(f"{year}年のレースID取得でエラー(page {page}): {e}")
                break
            
        return list(set(race_ids))

    def fetch_race_result(self, race_id):
        """1レースの結果と全着順、タイム、上がり3F、騎手、調教師などを取得"""
        url = f"https://db.netkeiba.com/race/{race_id}"
        logger.info(f"レース結果取得中: {race_id} ({url})")
        try:
            time.sleep(1.5)
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.raise_for_status()
            res.encoding = 'EUC-JP'
            soup = BeautifulSoup(res.text, 'html.parser')
            
            table = soup.select_one('table.race_table_01')
            if not table:
                logger.warning(f"レース結果テーブルが見つかりません: {race_id}")
                return None
                
            from io import StringIO
            df = pd.read_html(StringIO(str(table)), flavor='html5lib')[0]
            
            # ID抽出 (リスト作成して df に追加)
            horse_ids = []
            jockey_ids = []
            trainer_ids = []
            
            for tr in table.select('tr')[1:]: # ヘッダー行をスキップ
                tds = tr.select('td')
                if len(tds) > 3:
                    # 馬ID
                    h_a = tds[3].find('a', href=re.compile(r'/horse/'))
                    horse_ids.append(re.search(r'/horse/(\d+)', h_a['href']).group(1) if h_a else None)
                    # 騎手ID
                    j_a = tds[6].find('a', href=re.compile(r'/jockey/result/recent/'))
                    jockey_ids.append(re.search(r'/jockey/result/recent/(\d+)', j_a['href']).group(1) if j_a else None)
                    # 調教師ID (tds[18]付近か、aタグの中身を探す)
                    t_a = tr.find('a', href=re.compile(r'/trainer/result/recent/'))
                    trainer_ids.append(re.search(r'/trainer/result/recent/(\d+)', t_a['href']).group(1) if t_a else None)
                else:
                    horse_ids.append(None)
                    jockey_ids.append(None)
                    trainer_ids.append(None)
            
            # 行数チェック (取消馬などがいると合わない場合があるため長さ確認)
            if len(df) == len(horse_ids):
                df['horse_id'] = horse_ids
                df['jockey_id'] = jockey_ids
                df['trainer_id'] = trainer_ids
                
            df['race_id'] = race_id
            return df
            
        except Exception as e:
            logger.error(f"レース結果取得エラー {race_id}: {e}")
            return None

    def fetch_horse_pedigree(self, horse_id):
        """1頭の馬の血統（父・母父など）を取得"""
        # 血統専用ページにアクセス
        url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
        logger.info(f"血統データ取得中: {horse_id} ({url})")
        try:
            time.sleep(1.5)
            res = requests.get(url, headers=HEADERS, timeout=10)
            res.raise_for_status()
            res.encoding = 'EUC-JP'
            soup = BeautifulSoup(res.text, 'html.parser')
            
            blood_table = soup.select_one('table.blood_table')
            if not blood_table:
                logger.warning(f"血統テーブルが見つかりません: {horse_id}")
                return None
            
            # 父、母、母父の情報を取得
            # 5代血統表なので、tdタグの中に順番に入っている。
            # tds[0] が 父, tds[1] が 母, tds[2] が 父の父, tds[3] が 母の父 ... という独自の構造等の場合もあるが、
            # 一般的に netkeiba の db の blood_table は、aタグを順に抽出すると:
            # a[0] = 父, a[1] = 父の父, ..., a[14] = 母, a[15] = 母父
            # ここでは確実なクラス名か、tdsを使用。
            tds = blood_table.find_all('td')
            sire = tds[0].text.strip().replace('\n', '') if len(tds) > 0 else ""
            
            # 母、母父の正確な位置を探すのが面倒な場合、aタグで抽出するのが楽
            a_tags = blood_table.find_all('a')
            if len(a_tags) >= 16:
                sire = a_tags[0].text.strip()
                bms = a_tags[15].text.strip() # 15番目が母父 (BMS) になることが多い
            else:
                sire = "Unknown"
                bms = "Unknown"
            
            return {
                "horse_id": horse_id,
                "sire": sire,
                "bms": bms
            }
        except Exception as e:
            logger.error(f"血統データ取得エラー {horse_id}: {e}")
            return None

def dict_to_df(d):
    return pd.DataFrame([d])

def init_db(engine):
    """機械学習用の新しいテーブル構造を作成"""
    with engine.connect() as conn:
        # 重賞レース結果テーブル
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS historical_race_results (
                race_id TEXT,
                着順 TEXT,
                枠番 TEXT,
                馬番 TEXT,
                馬名 TEXT,
                性齢 TEXT,
                斤量 TEXT,
                騎手 TEXT,
                タイム TEXT,
                着差 TEXT,
                単勝 TEXT,
                人気 TEXT,
                馬体重 TEXT,
                調教師 TEXT,
                horse_id TEXT,
                jockey_id TEXT,
                trainer_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(race_id, horse_id)
            )
        '''))
        # 血統情報テーブル
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS horse_pedigrees (
                horse_id TEXT PRIMARY KEY,
                sire TEXT,
                bms TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        conn.commit()
    logger.info("データベーステーブルの初期化が完了しました。")

def main():
    logger.info("過去データ収集（重賞レース限定）バッチを開始します。")
    engine = get_engine()
    init_db(engine)
    session = get_session(engine)
    
    ingester = NetkeibaIngester(session)
    
    # 承認時のシステム仕様として、まずは直近3年分(2024~2026)をデータ収集対象とする
    # ユーザー要望により「過去5年分(2022~2026)」に変更
    years = [2022, 2023, 2024, 2025, 2026] 
    
    for year in years:
        race_ids = ingester.get_graderace_ids(year)
        logger.info(f"=== {year}年の重賞レースIDを {len(race_ids)}件 取得しました ===")
        
        for i, race_id in enumerate(race_ids):
            logger.info(f"[{i+1}/{len(race_ids)}] レース {race_id} の結果を取得します...")
            
            # 既にDBに登録済みかチェック
            with engine.connect() as conn:
                res = conn.execute(text("SELECT COUNT(*) FROM historical_race_results WHERE race_id = :r"), {"r": race_id}).scalar()
                if res and res > 0:
                    logger.info(f"レース {race_id} は既にDBに存在するためスキップします。")
                    continue
            
            df_result = ingester.fetch_race_result(race_id)
            
            if df_result is not None and not df_result.empty:
                # DBに保存
                try:
                    # カラム名の正規化：半角スペース・全角スペース・改行・タブをすべて除去
                    df_result.columns = [
                        re.sub(r'[\s\u3000]+', '', str(c)) for c in df_result.columns
                    ]
                    logger.debug(f"クレンジング後のカラム名: {df_result.columns.tolist()}")
                    
                    # 保存先テーブルに存在するカラムだけを抽出
                    target_cols = [
                        'race_id', '着順', '枠番', '馬番', '馬名', '性齢', '斤量', 
                        '騎手', 'タイム', '着差', '単勝', '人気', '馬体重', '調教師', 
                        'horse_id', 'jockey_id', 'trainer_id'
                    ]
                    # df_resultに存在するカラムだけ残す
                    available_cols = [c for c in target_cols if c in df_result.columns]
                    logger.debug(f"保存対象カラム: {available_cols}")
                    df_save = df_result[available_cols].astype(str)
                    
                    # INSERT OR IGNORE を使い、UNIQUE制約違反（重複）はスキップ
                    def insert_or_ignore(table, conn, keys, data_iter):
                        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                        stmt = sqlite_insert(table.table).values(list(data_iter))
                        stmt = stmt.on_conflict_do_nothing()
                        conn.execute(stmt)

                    df_save.to_sql(
                        'historical_race_results',
                        con=engine,
                        if_exists='append',
                        index=False,
                        method=insert_or_ignore
                    )
                    logger.info(f"レース {race_id} をDBに保存しました。（{len(df_save)}頭）")
                except Exception as e:
                    logger.error(f"DB保存エラー {race_id}: {e}")
                
                # 血統データの取得
                for _, row in df_result.iterrows():
                    horse_id = row.get('horse_id')
                    if not horse_id or pd.isna(horse_id) or horse_id == 'None':
                        continue
                        
                    # 既にDBに血統データがあるかチェック
                    with engine.connect() as conn:
                        res = conn.execute(text("SELECT COUNT(*) FROM horse_pedigrees WHERE horse_id = :h"), {"h": horse_id}).scalar()
                        if res and res > 0:
                            continue
                    
                    pedigree = ingester.fetch_horse_pedigree(horse_id)
                    if pedigree:
                        try:
                            dict_to_df(pedigree).to_sql('horse_pedigrees', con=engine, if_exists='append', index=False)
                            logger.info(f"血統情報を保存しました: {horse_id}")
                        except Exception as e:
                            logger.error(f"血統DB保存エラー {horse_id}: {e}")
            
    logger.info("=== 処理完了 ===")

if __name__ == "__main__":
    main()
