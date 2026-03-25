import pandas as pd
import logging
from strategy.bet_strategy import BettingStrategy

logger = logging.getLogger(__name__)

class Backtester:
    """
    過去の予測結果に対して、どれくらい儲かったかシミュレーションするクラス。
    """
    def __init__(self, initial_capital=100000):
        self.capital = initial_capital
        self.strategy = BettingStrategy()

    def run(self, df):
        """
        バックテストを実行する。
        df には 'proba'（AI予測）と '単勝'（実際の配当）が含まれている必要があります。
        """
        logger.info("バックテストを開始します...")
        
        # 期待値を計算して買い目を抽出
        df = self.strategy.calculate_ev(df)
        picks = df[df['ev'] >= self.strategy.target_ev]
        
        # 的中判定（'着 順' が 1 の馬が的中）
        picks['is_hit'] = (picks['着 順'] == 1).astype(int)
        
        # 収支計算（100円ずつ賭けたと仮定）
        bet_amount = 100
        total_bet = len(picks) * bet_amount
        total_return = (picks[picks['is_hit'] == 1]['単勝'] * bet_amount).sum()
        
        profit = total_return - total_bet
        roc = (total_return / total_bet) if total_bet > 0 else 0
        
        logger.info(f"結果： 投資額 {total_bet}円 / 払戻額 {total_return:.0f}円")
        logger.info(f"収支 {profit:.0f}円 / 回収率 {roc:.1%}")
        
        return {
            '投資額': total_bet,
            '払戻額': total_return,
            '収支': profit,
            '回収率': roc
        }

if __name__ == "__main__":
    # バックテスト用ダミーデータ
    test_history = pd.DataFrame({
        '馬 番': [1, 2, 3, 1, 2, 3],
        '単勝': [2.5, 10.0, 50.0, 1.5, 5.0, 20.0],
        'proba': [0.5, 0.15, 0.05, 0.8, 0.1, 0.05],
        '着 順': [1, 5, 12, 2, 1, 8] # 1R目は馬番1、2R目は馬番2が1着
    })
    
    tester = Backtester()
    tester.run(test_history)
