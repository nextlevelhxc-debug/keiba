import sys
import os
import logging
import json
import pandas as pd
from datetime import datetime

# インポートパスの設定
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from ingest.ingest_raceday import RaceDayIngester
from models.train_model import HorseRacingModel
from strategy.bet_strategy import BettingStrategy
from database import get_engine, get_session
from analysis.horse_scoring import HorseScorer
from analysis.jockey_scoring import JockeyScorer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_target_race_ids():
    # 週末の重賞レースやメインレースのIDを動的に取得するロジック（簡易版）
    # 本番では netkeiba 等のスクレイピングで動的取得が望ましいですが、ここではテスト用に固定
    nakayama_base = "2026060204"
    hanshin_base = "2026090106"
    return [f"{nakayama_base}11", f"{hanshin_base}11"]

def main():
    logger.info("=== 自動予想バッチ処理 起動 ===")
    
    engine = get_engine()
    session = get_session(engine)
    ingester = RaceDayIngester(session)
    scorer = HorseScorer(session)
    jockey_scorer = JockeyScorer(session)
    
    predictions_data = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "races": []
    }

    try:
        target_race_ids = generate_target_race_ids()
        
        for race_id in target_race_ids:
            place = "中山" if "06" in race_id[:6] else "阪神" # 簡易判定
            round_num = int(race_id[-2:])
            logger.info(f"--- {place} {round_num}R ({race_id}) の分析中 ---")
            
            entries = ingester.fetch_today_entries(race_id)
            if entries.empty:
                logger.warning(f"{race_id} の出馬表が取得できませんでした。")
                continue

            horse_abilities = []
            for _, row in entries.iterrows():
                h_id = row.get('horse_id')
                jockey_name = row.get('騎手')
                
                jockey_score_val = 50.0
                if jockey_name:
                    j_ability = jockey_scorer.get_scores(jockey_name)
                    if j_ability: jockey_score_val = j_ability.score
                
                speed, stamina, explosiveness, consistency, experience = 50.0, 50.0, 50.0, 50.0, 20.0
                if h_id:
                    ability = scorer.get_scores(h_id)
                    if ability:
                        speed, stamina, explosiveness, consistency, experience = (
                            ability.speed, ability.stamina, ability.explosiveness, ability.consistency, ability.experience
                        )
                
                total_score = speed + stamina + explosiveness + consistency + experience + jockey_score_val
                
                # int64などを標準のint/floatにキャストしてJSONシリアライズ可能にする
                horse_abilities.append({
                    'horse_number': int(row.get('馬番', 0)),
                    'horse_name': str(row.get('馬名', '')),
                    'jockey_name': str(jockey_name),
                    'speed': float(speed),
                    'stamina': float(stamina),
                    'explosiveness': float(explosiveness),
                    'consistency': float(consistency),
                    'experience': float(experience),
                    'jockey_score': float(jockey_score_val),
                    'total_score': float(total_score)
                })
            
            ability_df = pd.DataFrame(horse_abilities)
            # 順位付け
            ability_df = ability_df.sort_values('total_score', ascending=False).reset_index(drop=True)
            
            top_horses = ability_df.head(3).to_dict(orient='records')
            recommended_bet = f"馬連 {top_horses[0]['horse_number']} - {top_horses[1]['horse_number']} (保険: {top_horses[0]['horse_number']} - {top_horses[2]['horse_number']})"
            
            race_info = {
                "race_id": race_id,
                "place": place,
                "round": round_num,
                "top_pickup": top_horses[0], # イチオシ馬
                "top_3_horses": top_horses,
                "recommended_bet": recommended_bet,
                "all_horses": ability_df.to_dict(orient='records')
            }
            predictions_data["races"].append(race_info)

        # JSONファイルとして出力
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(project_root, "data")
        os.makedirs(data_dir, exist_ok=True)
        
        output_file = os.path.join(data_dir, "latest_prediction.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(predictions_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"✅ 予測結果を {output_file} に保存しました。")

    except Exception as e:
        logger.error(f"❌ エラーが発生しました: {str(e)}")
        sys.exit(1)
        
    finally:
        session.close()
        logger.info("=== バッチ処理が完了しました ===")

if __name__ == "__main__":
    main()
