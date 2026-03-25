import pandas as pd
import numpy as np
import re

def parse_age_gender(gender_age_str):
    """
    「牡3」「牝4」「セ5」のような文字列を性別と年齢に分ける関数。
    """
    if not isinstance(gender_age_str, str):
        return None, None
    
    # 性別（最初の1文字）
    gender = gender_age_str[0]
    # 年齢（残りの数字部分）
    age_match = re.search(r'\d+', gender_age_str)
    age = int(age_match.group()) if age_match else None
    
    return gender, age

def parse_horse_weight(weight_str):
    """
    「480(+2)」「500(-10)」のような文字列を現在の体重と、増減値に分ける関数。
    """
    if not isinstance(weight_str, str) or weight_str == '計不':
        return None, None
    
    # 体重 (例: 480)
    current_weight_match = re.match(r'(\d+)', weight_str)
    current_weight = int(current_weight_match.group(1)) if current_weight_match else None
    
    # 増減 (例: +2, -10)
    diff_match = re.search(r'\(([\+\-]?\d+)\)', weight_str)
    weight_diff = int(diff_match.group(1)) if diff_match else 0
    
    return current_weight, weight_diff

def calculate_popularity_deviation(df):
    """
    「人気」と「単勝オッズ」の乖離（かいり）を計算する関数。
    穴馬（実力の割に人気がない馬）を見つけるための指標になります。
    """
    # 期待されるオッズ（単純なモデルとして人気の逆数などを使う場合もありますが、
    # ここでは既存の人気と実際のオッズの差をみます）
    # 例：1番人気なのにオッズが高い、などは過小評価の可能性があります。
    
    # ここでは単純に「オッズ / 人気」の比率を出してみます。
    # この値が高いほど、人気の割にオッズがついており「穴馬」の可能性があります。
    df['popularity_odds_ratio'] = df['単勝'] / df['人 気']
    return df

def preprocess_results(df):
    """
    スクレイピングしてきたデータフレーム(df)をAIが読みやすい形に加工するメインの関数。
    """
    # 列名のコピー（元のデータを壊さないため）
    df = df.copy()
    
    # 1. 性別と年齢の加工
    if '性齢' in df.columns:
        df['gender'], df['age'] = zip(*df['性齢'].map(parse_age_gender))
        # 性別を数字に変換（牡:0, 牝:1, セ:2 など）
        gender_map = {'牡': 0, '牝': 1, 'セ': 2}
        df['gender_val'] = df['gender'].map(gender_map)
    
    # 2. 馬体重の加工
    if '馬体重' in df.columns:
        df['horse_weight'], df['weight_diff'] = zip(*df['馬体重'].map(parse_horse_weight))
    
    # 3. 人気乖離の計算
    if '単勝' in df.columns and '人 気' in df.columns:
        # 文字列を数値に変換（エラーが出る場合はNaNにする）
        df['単勝'] = pd.to_numeric(df['単勝'], errors='coerce')
        df['人 気'] = pd.to_numeric(df['人 気'], errors='coerce')
        df = calculate_popularity_deviation(df)
        
    return df

if __name__ == "__main__":
    # テスト用のサンプルデータ
    test_data = pd.DataFrame({
        '馬名': ['ドウデュース', 'イクイノックス'],
        '性齢': ['牡3', '牡3'],
        '馬体重': ['500(+2)', '490(-4)'],
        '単勝': ['1.1', '3.5'],
        '人 気': ['1', '2']
    })
    
    print("--- 加工前 ---")
    print(test_data)
    
    processed = preprocess_results(test_data)
    
    print("\n--- 加工後 ---")
    print(processed[['馬名', 'gender', 'age', 'horse_weight', 'weight_diff', 'popularity_odds_ratio']])
