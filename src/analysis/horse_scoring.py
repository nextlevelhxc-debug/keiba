import logging
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import func
import sys
import os

# パス追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Horse, Result, Race, HorseAbility

logger = logging.getLogger(__name__)

class HorseScorer:
    """
    馬の過去成績に基づいて5項目の能力値を算出・更新するクラス
    """
    def __init__(self, db_session: Session):
        self.session = db_session

    def calculate_and_update(self, horse_id: str):
        """
        特定の馬の能力を計算し、DBを更新する
        """
        results = self.session.query(Result).join(Race).filter(Result.horse_id == horse_id).order_by(Race.date.desc()).all()
        
        if not results:
            return None

        # --- 直近重視の変更 ---
        # 1. スピード (最新の平均タイムを距離で正規化)
        # スピード = 平均(距離 / 秒) * 係数
        speeds = []
        for r in results:
            if r.time_seconds and r.race.distance:
                speeds.append(r.race.distance / r.time_seconds)
        speed_score = (sum(speeds) / len(speeds)) * 5 if speeds else 50.0

        # 2. スタミナ (最長距離実績と着順を考慮)
        distances = [r.race.distance for r in results if r.rank and r.rank <= 5]
        stamina_score = (max(distances) / 3200) * 100 if distances else 50.0

        # 3. 瞬発力 (上がり3Fの平均を正規化)
        last_3fs = [r.last_3f for r in results if r.last_3f]
        if last_3fs:
            avg_3f = sum(last_3fs) / len(last_3fs)
            # 34秒を100点、40秒を0点とする簡易計算
            explosiveness_score = max(0, min(100, (40 - avg_3f) * 16.6))
        else:
            explosiveness_score = 50.0

        # 4. 安定性 (平均着順の逆数)
        ranks = [r.rank for r in results if r.rank]
        if ranks:
            avg_rank = sum(ranks) / len(ranks)
            # 1位を100点、16位を0点とする
            consistency_score = max(0, min(100, (16 - avg_rank) * 6.6))
        else:
            consistency_score = 50.0

        # 5. 実績 (レース格付けと勝利数)
        wins = len([r for r in results if r.rank == 1])
        experience_score = min(100, wins * 20 + len(results) * 2)

        # DB更新
        ability = self.session.query(HorseAbility).filter_by(horse_id=horse_id).first()
        if not ability:
            ability = HorseAbility(horse_id=horse_id)
            self.session.add(ability)

        ability.speed = round(speed_score, 1)
        ability.stamina = round(stamina_score, 1)
        ability.explosiveness = round(explosiveness_score, 1)
        ability.consistency = round(consistency_score, 1)
        ability.experience = round(experience_score, 1)
        ability.last_updated = date.today()

        self.session.commit()
        logger.info(f"馬ID {horse_id} の能力スコアを更新しました。")
        return ability

    def get_scores(self, horse_id: str):
        """
        保存されているスコアを取得する。なければ計算する。
        """
        ability = self.session.query(HorseAbility).filter_by(horse_id=horse_id).first()
        if not ability:
            return self.calculate_and_update(horse_id)
        return ability
