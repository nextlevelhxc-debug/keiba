import logging
from datetime import date
from sqlalchemy.orm import Session
import sys
import os

# パス追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Result, Race, JockeyAbility

logger = logging.getLogger(__name__)

class JockeyScorer:
    """
    騎手の過去成績に基づいて能力値（スコア）を算出・更新するクラス
    """
    def __init__(self, db_session: Session):
        self.session = db_session

    def calculate_and_update(self, jockey_name: str):
        """
        特定の騎手の能力を計算し、DBを更新する
        """
        if not jockey_name:
            return None

        # 騎手の全成績を取得
        results = self.session.query(Result).join(Race).filter(Result.jockey == jockey_name).order_by(Race.date.desc()).all()
        
        if not results:
            return None

        total_races = len(results)
        wins = len([r for r in results if r.rank == 1])
        top3 = len([r for r in results if r.rank and r.rank <= 3])

        win_rate = (wins / total_races) * 100 if total_races > 0 else 0.0
        top3_rate = (top3 / total_races) * 100 if total_races > 0 else 0.0

        # スコア計算: 勝率と複勝率をベースに、経験数を加味（最大100点程度）
        # 例: 勝率15% -> 30点、複勝率35% -> 35点、経験数ボーナス（最大35点）
        base_score = (win_rate * 2.0) + top3_rate
        experience_bonus = min(35.0, total_races * 0.5) 
        score = min(100.0, base_score + experience_bonus)

        # DB更新
        ability = self.session.query(JockeyAbility).filter_by(jockey_name=jockey_name).first()
        if not ability:
            ability = JockeyAbility(jockey_name=jockey_name)
            self.session.add(ability)

        ability.score = round(score, 1)
        ability.win_rate = round(win_rate, 1)
        ability.top3_rate = round(top3_rate, 1)
        ability.races_run = total_races
        ability.last_updated = date.today()

        self.session.commit()
        logger.info(f"騎手 [{jockey_name}] の能力スコアを更新しました。(スコア: {ability.score})")
        return ability

    def get_scores(self, jockey_name: str):
        """
        保存されているスコアを取得する。なければ計算する。
        """
        if not jockey_name:
            return None
            
        ability = self.session.query(JockeyAbility).filter_by(jockey_name=jockey_name).first()
        # 今回の対応では、常に最新のデータベース状況から計算するようにしてもよいが、一旦キャッシュ利用
        if not ability:
            return self.calculate_and_update(jockey_name)
        return ability

# テスト用
if __name__ == "__main__":
    from database import get_engine, get_session
    logging.basicConfig(level=logging.INFO)
    engine = get_engine()
    session = get_session(engine)
    scorer = JockeyScorer(session)
    # ルメール騎手などでテスト
    scorer.calculate_and_update("ルメール")
    session.close()
