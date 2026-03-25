import sys
import os
import logging
import time
import pandas as pd

sys.path.append(os.path.abspath('src'))
from database import get_engine, get_session
from ingest.ingest_jravan import DataIngester
from ingest.ingest_raceday import RaceDayIngester
from analysis.horse_scoring import HorseScorer
from analysis.jockey_scoring import JockeyScorer
from ingest.ingest_nakayama11_horses import fetch_past_races_for_horse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    race_id = "202607010311" # 中京 1回 3日目 11R (ファルコンS G3)
    engine = get_engine()
    session = get_session(engine)
    
    raceday_ingester = RaceDayIngester(session)
    ingester = DataIngester(session)
    scorer = HorseScorer(session)
    jockey_scorer = JockeyScorer(session)
    
    logger.info(f"=== 中京11R ({race_id}) の出馬表を取得 ===")
    entries = raceday_ingester.fetch_today_entries(race_id)
    if entries.empty:
        logger.error("出馬表が取得できませんでした。")
        return
        
    horse_ids = entries['horse_id'].dropna().tolist()
    logger.info(f"出走馬 {len(horse_ids)} 頭の過去成績を収集しスコアを計算します。")
    
    for h_id in horse_ids:
        fetch_past_races_for_horse(h_id, ingester, scorer)
        time.sleep(1.0)
        
    logger.info("=== 中京11R ファルコンS レース予想 ===")
    horse_abilities = []
    
    for _, row in entries.iterrows():
        h_id = row.get('horse_id')
        jockey_name = row.get('騎手')
        
        jockey_score_val = 50.0
        if jockey_name:
            j_ability = jockey_scorer.get_scores(jockey_name)
            if j_ability:
                jockey_score_val = j_ability.score
                
        speed, stamina, explosiveness, consistency, experience = 50.0, 50.0, 50.0, 50.0, 20.0
        if h_id:
            ability = scorer.get_scores(h_id)
            if ability:
                speed, stamina, explosiveness, consistency, experience = (
                    ability.speed, ability.stamina, ability.explosiveness, ability.consistency, ability.experience
                )
                
        total_score = speed + stamina + explosiveness + consistency + experience + jockey_score_val
        
        horse_abilities.append({
            'horse_id': h_id,
            'jockey_name': jockey_name,
            'speed': round(speed, 1),
            'stamina': round(stamina, 1),
            'explosiveness': round(explosiveness, 1),
            'consistency': round(consistency, 1),
            'experience': round(experience, 1),
            'jockey_score': round(jockey_score_val, 1),
            'total_score': round(total_score, 1)
        })
        
    ability_df = pd.DataFrame(horse_abilities)
    data = pd.concat([entries.reset_index(drop=True), ability_df.reset_index(drop=True)], axis=1)
    
    # 総合力でソート降順
    data = data.sort_values('total_score', ascending=False).reset_index(drop=True)
    
    print("\n==================================================")
    print("【中京11R ファルコンS (G3) 実力ベース順位予想】")
    print("==================================================")
    
    for i, row in data.head(5).iterrows():
        if i < 3:
            rank_name = ["1着 (◎)", "2着 (○)", "3着 (▲)"][i]
        else:
            rank_name = f"{i+1}着"
            
        print(f"{rank_name}: 馬番{row['馬番']} {row['馬名']} (騎手: {row['jockey_name']})")
        print(f"   [馬スコア] スピ:{row['speed']} スタ:{row['stamina']} 瞬:{row['explosiveness']} 安定:{row['consistency']} 実績:{row['experience']}")
        print(f"   [騎手スコア] {row['jockey_score']}  => [総合力] {row['total_score']}")
    
    print("--------------------------------------------------")
    print("--- 全出走馬 総合力一覧 ---")
    for _, row in data.iterrows():
        print(f"馬番{row['馬番']}: {row['馬名']} / 騎: {row['jockey_name']} (総合: {row['total_score']})")
        
    session.close()

if __name__ == "__main__":
    main()
