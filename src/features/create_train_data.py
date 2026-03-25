import os
import sys
import pandas as pd
import logging

# 上位ディレクトリのモジュールをインポートするためのパス追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_engine, get_session, Race, Result
from features.features import preprocess_results

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_from_db():
    """
    データベースからレース結果を読み込んで、一つのデータフレームにまとめる関数。
    """
    engine = get_engine()
    
    # SQLを使ってデータを取得（本当はもっと複雑な結合をしますが、まずはシンプルに）
    # racesテーブルとresultsテーブルを結合して取得します
    query = """
    SELECT 
        r.id as race_id, r.date, r.course_type, r.distance,
        res.rank as '着 順', res.horse_number as '馬 番', res.odds as '単勝', res.popularity as '人 気'
    FROM races r
    JOIN results res ON r.id = res.race_id
    """
    
    # pandasのread_sqlを使うと直接データフレームとして読み込めます
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"データベースから {len(df)} 件のデータを読み込みました。")
        return df
    except Exception as e:
        logger.error(f"データ読み込み中にエラーが発生しました: {e}")
        return pd.DataFrame()

def main():
    logger.info("=== 学習用データの作成を開始します ===")
    
    # 1. データの読み込み
    df = load_data_from_db()
    
    if df.empty:
        logger.warning("データが空です。処理を中断します。")
        return
    
    # 2. 特徴量生成（さっき作ったpreprocess_resultsを適用）
    logger.info("特徴量生成を適用中...")
    processed_df = preprocess_results(df)
    
    # 3. CSVとして保存（これがAIの学習用「教科書」になります）
    output_path = "data/training_data.csv"
    processed_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"学習用データを保存しました: {output_path}")
    
    logger.info("=== 処理が完了しました ===")

if __name__ == "__main__":
    main()
