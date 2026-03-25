import pandas as pd
import logging

logger = logging.getLogger(__name__)

class BettingStrategy:
    """
    AIの予測を元に、どの買い目をいくら買うか決めるクラス。
    """
    def __init__(self, target_ev=1.1):
        self.target_ev = target_ev # 期待値がこの値を超えたら「買い」

    def calculate_ev(self, df):
        """
        期待値（Expected Value = 予測確率 × オッズ）を計算する。
        """
        # 単勝期待値を計算
        df['ev'] = df['proba'] * df['単勝']
        return df

    def generate_bets(self, df):
        """
        具体的な買い目を提案する。
        """
        # 期待値が高い順に並び替え
        df = df.sort_values('ev', ascending=False)
        
        # 期待値が閾値を超えている馬を抽出
        bets = df[df['ev'] >= self.target_ev]
        
        results = []
        for idx, row in bets.iterrows():
            results.append({
                '馬名': row['馬名'] if '馬名' in row else f"馬番{row['馬 番']}",
                '予測確率': f"{row['proba']:.1%}",
                '単勝オッズ': row['単勝'],
                '期待値': f"{row['ev']:.2f}"
            })
        
        return results

if __name__ == "__main__":
    # テストデータ
    test_results = pd.DataFrame({
        '馬 番': [1, 2, 3],
        '単勝': [2.5, 10.0, 50.0],
        'proba': [0.5, 0.15, 0.05] # AIの予測（50%, 15%, 5%）
    })
    
    strategy = BettingStrategy()
    test_results = strategy.calculate_ev(test_results)
    picks = strategy.generate_bets(test_results)
    
    print("--- 推奨買い目 ---")
    for pick in picks:
        print(pick)
