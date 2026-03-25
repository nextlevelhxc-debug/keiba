import sys
import os
import logging
import pandas as pd

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

def generate_today_race_ids():
    nakayama_base = "2026060204"
    hanshin_base = "2026090106"
    race_ids = []
    # テストとして、11R (メインレース) のみ表示するように絞ります（全レース出力は長いので）
    # ユーザーが「全レース詳細を教えて」と言った場合は全レースループしますが、
    # ここでは要望に合わせて中山・阪神の11Rをメインに、全体構成を維持します。
    for i in range(11, 12): # 中山11R
        race_ids.append(f"{nakayama_base}{i:02d}")
    for i in range(11, 12): # 阪神11R
        race_ids.append(f"{hanshin_base}{i:02d}")
    return race_ids

def main():
    logger.info("=== 実力ベース・純粋順位予想システム 起動 ===")
    
    engine = get_engine()
    session = get_session(engine)
    ingester = RaceDayIngester(session)
    scorer = HorseScorer(session)
    jockey_scorer = JockeyScorer(session) # 騎手スコアラーを追加
    
    # AIモデル（確率は参考値として保持）
    model = HorseRacingModel()

    try:
        today_race_ids = generate_today_race_ids()
        
        for race_id in today_race_ids:
            place = "中山" if "060204" in race_id else "阪神"
            round_num = int(race_id[-2:])
            logger.info(f"--- {place} {round_num}R ({race_id}) の分析中 ---")
            
            entries = ingester.fetch_today_entries(race_id)
            if entries.empty: continue

            # 能力スコアの取得と計算
            horse_abilities = []
            for _, row in entries.iterrows():
                h_id = row.get('horse_id')
                jockey_name = row.get('騎手') # スクレイピングされた出馬表から騎手名を取得
                
                # 騎手スコアの計算
                jockey_score_val = 50.0 # デフォルト
                if jockey_name:
                    j_ability = jockey_scorer.get_scores(jockey_name)
                    if j_ability:
                        jockey_score_val = j_ability.score
                
                # 馬スコアの計算
                speed, stamina, explosiveness, consistency, experience = 50.0, 50.0, 50.0, 50.0, 20.0
                if h_id:
                    ability = scorer.get_scores(h_id)
                    if ability:
                        speed, stamina, explosiveness, consistency, experience = (
                            ability.speed, ability.stamina, ability.explosiveness, ability.consistency, ability.experience
                        )
                
                # トータルスコア (馬の能力 + 騎手の能力)
                total_score = speed + stamina + explosiveness + consistency + experience + jockey_score_val
                
                horse_abilities.append({
                    'horse_id': h_id,
                    'jockey_name': jockey_name,
                    'speed': speed,
                    'stamina': stamina,
                    'explosiveness': explosiveness,
                    'consistency': consistency,
                    'experience': experience,
                    'jockey_score': jockey_score_val,
                    'total_score': total_score
                })
            
            ability_df = pd.DataFrame(horse_abilities)
            data = pd.concat([entries.reset_index(drop=True), ability_df.reset_index(drop=True)], axis=1)

            # 順位付け (total_scoreの降順)
            data = data.sort_values('total_score', ascending=False).reset_index(drop=True)

            # 結果出力
            print(f"\n【{place} {round_num}R 実力ベース順位予想 (騎手データ反映版)】")
            print("--------------------------------------------------")
            for i, row in data.head(3).iterrows():
                rank_name = ["1着 (◎)", "2着 (○)", "3着 (▲)"][i]
                print(f"{rank_name}: 馬番{row['馬番']} {row['馬名']} (騎手: {row['jockey_name']})")
                print(f"   [馬スコア] スピード:{row['speed']} スタミナ:{row['stamina']} 瞬発力:{row['explosiveness']} 安定性:{row['consistency']} 実績:{row['experience']}")
                print(f"   [騎手スコア] {row['jockey_score']}  => [総合力] {row['total_score']:.1f}")
            print("--------------------------------------------------")
            
            # 全馬の簡易ステータス
            print("--- 全馬の能力合計値 ---")
            for _, row in data.iterrows():
                print(f"馬番{row['馬番']}: {row['馬名']} / 騎手:{row['jockey_name']} (総合: {row['total_score']:.1f})")

    finally:
        session.close()
        logger.info("=== 予想処理が完了しました ===")

if __name__ == "__main__":
    main()
