import sys
import os
import sqlite3
import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, text

# プロジェクトルートを追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_engine, get_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_raw_data(engine):
    """データベースから生データを読み込む"""
    logger.info("データベースから学習用生データを読み込んでいます...")
    
    # 過去のレース結果と血統をJOINして取得
    query = """
    SELECT 
        r.*,
        p.sire,
        p.bms
    FROM historical_race_results r
    LEFT JOIN horse_pedigrees p ON r.horse_id = p.horse_id
    WHERE r.着順 != '取消' AND r.着順 != '除外' AND r.着順 != '中止'
    """
    
    try:
        df = pd.read_sql(query, con=engine)
        return df
    except Exception as e:
        logger.error(f"データ読み込みエラー: {e}")
        return pd.DataFrame()

def preprocess_and_clean(df):
    """基礎的なデータクリーニングと型変換"""
    logger.info("データのクリーニングと型変換を実行中...")
    
    # 着順を数値化（1(降) などの判定を除外して純粋な数字に）
    df['rank'] = df['着順'].astype(str).str.extract(r'(\d+)').astype(float)
    
    # ターゲット変数 (目的変数)： 3着以内なら1、それ以外は0 (複勝率モデル用)
    # 単勝率モデル用には 1着なら1 (rank == 1) も用意する
    df['target_win'] = (df['rank'] == 1).astype(int)
    df['target_place'] = (df['rank'] <= 3).astype(int)
    
    # 馬番・枠番
    df['umaban'] = pd.to_numeric(df['馬番'], errors='coerce')
    df['wakuban'] = pd.to_numeric(df['枠番'], errors='coerce')
    
    # 斤量
    df['weight_carried'] = pd.to_numeric(df['斤量'], errors='coerce')
    
    # 人気・単勝オッズ
    df['popularity'] = pd.to_numeric(df['人気'], errors='coerce')
    df['odds'] = pd.to_numeric(df['単勝'], errors='coerce')
    
    # 馬体重と増減 (例: "480(+2)" -> 480, 2)
    def extract_weight(w_str):
        if pd.isna(w_str) or w_str == '計不':
            return np.nan, np.nan
        match = re.match(r'(\d+)\(([-+]\d+|0)\)', str(w_str))
        if match:
            return float(match.group(1)), float(match.group(2))
        return float(str(w_str).replace(' ', '')), 0.0

    import re
    weights = df['馬体重'].apply(extract_weight)
    df['horse_weight'] = [w[0] for w in weights]
    df['weight_change'] = [w[1] for w in weights]
    
    # タイムを秒に変換 (例: 1:33.4 -> 93.4)
    def time_to_sec(t_str):
        if pd.isna(t_str):
            return np.nan
        match = re.match(r'(\d+):(\d+\.\d+)', str(t_str))
        if match:
            return float(match.group(1)) * 60 + float(match.group(2))
        return np.nan
    df['time_sec'] = df['タイム'].apply(time_to_sec)
    
    # 上がり3F
    df['last_3f'] = pd.to_numeric(df['上り'], errors='coerce')
    
    # 性齢の分離 (例: "牡3" -> "牡", 3)
    df['sex'] = df['性齢'].str[0]
    df['age'] = pd.to_numeric(df['性齢'].str[1:], errors='coerce')
    
    # 基礎的な欠損値処理
    df.fillna({'horse_weight': df['horse_weight'].mean(), 'weight_change': 0}, inplace=True)
    
    return df

def build_features(df):
    """
    4つの高度な特徴量（指数）を生成する
    1. タイム指数 (Time Index)
    2. 血統適性指数 (Pedigree Aptitude Index)
    3. 相性指数 (Affinity Index: 騎手×調教師)
    4. 上がり3F順位指数 (Last 3F Rank Index)
    """
    logger.info("高度な特徴量（指数）を構築しています...")
    
    # --- 1. タイム指数ベース (レースごとの平均タイムとの差で補正) ---
    # 同一レース内でのタイム偏差値を簡易タイム指数とする
    df['time_index'] = df.groupby('race_id')['time_sec'].transform(lambda x: (x.mean() - x) / x.std() * 10 + 50)
    df['time_index'] = df['time_index'].fillna(50) # 欠損または1頭のみの場合は50
    
    # --- 2. 上がり3F順位指数 ---
    # レース内での上がりタイムの偏差値
    df['last_3f_index'] = df.groupby('race_id')['last_3f'].transform(lambda x: (x.mean() - x) / x.std() * 10 + 50)
    df['last_3f_index'] = df['last_3f_index'].fillna(50)
    
    # --- 3. 血統適性指数 (簡易版: 全体における父・母父の複勝率) ---
    # 本来はコースや距離ごとの集計が望ましいが、まずは単純な全体勝率を指数化する
    # ※リークを防ぐため、本来はターゲットエンコーディングの際にK-Fold等を使うが、今回はLightGBMに投げるためのベース特徴量とする
    sire_win_rate = df.groupby('sire')['target_place'].mean().to_dict()
    bms_win_rate = df.groupby('bms')['target_place'].mean().to_dict()
    df['pedigree_index'] = df['sire'].map(sire_win_rate).fillna(0.2) * 0.6 + df['bms'].map(bms_win_rate).fillna(0.2) * 0.4
    df['pedigree_index'] = df['pedigree_index'] * 100 # ％スケール
    
    # --- 4. 騎手・調教師 相性指数 ---
    # 騎手と調教師の組み合わせの過去勝率を指数化
    df['jt_combo'] = df['jockey_id'].astype(str) + "_" + df['trainer_id'].astype(str)
    combo_win_rate = df.groupby('jt_combo')['target_place'].mean().to_dict()
    df['affinity_index'] = df['jt_combo'].map(combo_win_rate).fillna(0.2) * 100
    
    # --- おまけ: 妙味指数 (期待値検索用) ---
    # 予想と結果を使う妙味の発見にはモデル予測が必要だが、特徴量として「人気に対してオッズが高いか」等を持たせてもよい
    
    return df

def save_features(df, engine):
    """生成した特徴量セットをDBまたはCSVに保存"""
    logger.info("生成した学習用特徴量を保存しています...")
    try:
        # DBに保存
        df.to_sql('ml_features', con=engine, if_exists='replace', index=False)
        logger.info(f"ml_features テーブルに {len(df)} 件の学習データを保存しました。")
    except Exception as e:
        logger.error(f"特徴量データの保存エラー: {e}")

def main():
    engine = get_engine()
    df_raw = load_raw_data(engine)
    
    if df_raw.empty:
        logger.warning("DBに学習データが存在しません。先にデータ収集 (ingest_historical_heavy.py) を実行してください。")
        return
        
    df_clean = preprocess_and_clean(df_raw)
    df_features = build_features(df_clean)
    
    save_features(df_features, engine)
    logger.info("特徴量エンジニアリング（AIの脳みそ作成準備）が完了しました。次はモデル学習 (train_lgbm.py) です。")

if __name__ == "__main__":
    main()
