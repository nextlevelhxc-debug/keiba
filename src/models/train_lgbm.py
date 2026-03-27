import sys
import os
import sqlite3
import pandas as pd
import numpy as np
import logging
import pickle
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine

# プロジェクトルートを追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_engine, get_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_features(engine):
    """学習用の特徴量データを読み込む"""
    logger.info("特徴量データ (ml_features) を読み込んでいます...")
    try:
        # race_id (例: 202409050811) で並び替えることで、過去のデータで学習し、
        # 直近のデータで検証(アーリーストッピング)できるようにする
        df = pd.read_sql("SELECT * FROM ml_features ORDER BY race_id", con=engine)
        return df
    except Exception as e:
        logger.error(f"特徴量データ読み込みエラー: {e}")
        return pd.DataFrame()

def train_model(df, target_col, model_path):
    """指定されたターゲット(勝率用 or 複勝率用)のLightGBMモデルを学習する"""
    logger.info(f"モデル学習開始: ターゲット={target_col}")
    
    # 学習に使用する特徴量カラム（指数・数値データのみ）
    feature_cols = [
        'umaban', 'wakuban', 'weight_carried', 'horse_weight', 'weight_change',
        'age', 'time_index', 'last_3f_index', 'pedigree_index', 'affinity_index'
    ]
    
    # 欠損値があれば中央値で穴埋め等の前処理
    # LightGBMは欠損値をそのまま扱えるが、明示的にNaNのまま渡す
    X = df[feature_cols]
    y = df[target_col]
    
    # 時系列スプリット（過去80%を学習、直近20%を検証用：アーリーストッピング用）
    # shuffle=False により、過去データから未来のデータを予測する形式を維持する
    X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    logger.info(f"学習データ: {len(X_train)}件, 検証データ: {len(X_valid)}件")
    
    # LightGBMデータセットの作成
    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_valid = lgb.Dataset(X_valid, y_valid, reference=lgb_train)
    
    # パラメータ設定 (二値分類 / 確率出力)
    params = {
        'objective': 'binary',
        'metric': 'binary_logloss',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'max_depth': 5,
        'feature_fraction': 0.8,
        'verbose': -1,
        'random_state': 42
    }
    
    logger.info("LightGBMモデルの学習を実行します...")
    
    # アーリーストッピングを使って過学習を防ぐ
    callbacks = [
        lgb.early_stopping(stopping_rounds=50, verbose=True),
        lgb.log_evaluation(period=50)
    ]
    
    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=1000,
        valid_sets=[lgb_train, lgb_valid],
        valid_names=['train', 'valid'],
        callbacks=callbacks
    )
    
    # 特徴量の重要度を出力
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    logger.info(f"【{target_col}モデルの特徴量重要度】\n{importance}")
    
    # モデルの保存
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
        
    logger.info(f"✅ モデルを保存しました: {model_path}")
    return model

def main():
    logger.info("=== LightGBM モデル学習パイプライン開始 ===")
    engine = get_engine()
    df = load_features(engine)
    
    if df.empty:
        logger.warning("学習用データがありません。先にデータ収集と特徴量生成を実行してください。")
        return
        
    # 保存先ディレクトリ
    model_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'models')
    
    # 1. 1着勝率予測モデル (単勝推奨馬用)
    train_model(df, target_col='target_win', model_path=os.path.join(model_dir, 'lgbm_win_model.pkl'))
    
    # 2. 3着内複勝率予測モデル (3連複軸馬・大穴用)
    train_model(df, target_col='target_place', model_path=os.path.join(model_dir, 'lgbm_place_model.pkl'))
    
    logger.info("=== すべてのモデルの学習と保存が完了しました ===")

if __name__ == "__main__":
    main()
