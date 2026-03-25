import pandas as pd
from catboost import CatBoostClassifier, Pool
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HorseRacingModel:
    """
    CatBoostアルゴリズムを使用した予測モデルクラス。
    """
    def __init__(self, model_path='data/horse_model.cbm'):
        self.model_path = model_path
        self.model = CatBoostClassifier(
            iterations=500,
            learning_rate=0.05,
            depth=6,
            loss_function='Logloss',
            verbose=100
        )

    def train(self, df):
        """
        AIを学習させるメソッド。
        """
        logger.info("学習を開始します...")
        
        # ターゲット（正解ラベル）：3着以内なら1、それ以外は0
        # 実際には df['着 順'] <= 3 などで作成
        if 'target' not in df.columns:
            df['target'] = (df['着 順'].astype(float) <= 3).astype(int)
        
        # 学習に使う特徴量（数値データのみ）
        features = ['age', 'horse_weight', 'weight_diff', 'popularity_odds_ratio']
        X = df[features].fillna(0)
        y = df['target']
        
        self.model.fit(X, y)
        self.model.save_model(self.model_path)
        logger.info(f"モデルを保存しました: {self.model_path}")

    def predict_proba(self, X):
        """
        「3着以内に入る確率」を予測する。
        """
        if os.path.exists(self.model_path):
            self.model.load_model(self.model_path)
        
        # クラス1（3着以内）である確率を返す
        return self.model.predict_proba(X)[:, 1]

if __name__ == "__main__":
    # テスト用の簡易データ
    test_df = pd.DataFrame({
        'age': [3, 4, 5, 3, 4],
        'horse_weight': [480, 500, 520, 490, 510],
        'weight_diff': [2, -4, 0, 6, -2],
        'popularity_odds_ratio': [1.1, 2.5, 0.8, 1.5, 3.2],
        '着 順': [1, 5, 2, 8, 3]
    })
    
    trainer = HorseRacingModel()
    trainer.train(test_df)
