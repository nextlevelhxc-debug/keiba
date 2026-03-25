import streamlit as st
import os
import json

# ページの設定（スマートフォンでも見やすくするため）
st.set_page_config(page_title="競馬予想アプリ", page_icon="🐴", layout="wide")

st.title("🐴 競馬予想アプリ (完全オート版)")

# パスワード認証
password = st.text_input("パスワードを入力してください", type="password")
correct_password = "keiba" # 初心者向けに簡単なものに設定

if password == correct_password:
    st.success("認証成功！ようこそ！")
    st.markdown("---")
    
    # 予想データの読み込み
    base_dir = os.path.abspath(os.path.dirname(__file__))
    data_file = os.path.join(base_dir, "data", "latest_prediction.json")
    
    if not os.path.exists(data_file):
        st.warning("⚠️ まだ今週の予想データが作成されていません。")
    else:
        with open(data_file, 'r', encoding='utf-8') as f:
            pred_data = json.load(f)
            
        st.caption(f"🔄 最終更新日時: {pred_data.get('generated_at', '不明')}")
        
        races = pred_data.get("races", [])
        if not races:
            st.info("予想されたレースがありません。")
        else:
            # タブで各レースを表示
            tabs = st.tabs([f"{r['place']}{r['round']}R" for r in races])
            
            for i, race in enumerate(races):
                with tabs[i]:
                    st.header(f"🎯 {race['place']} {race['round']}R")
                    
                    # 推奨買い目をデカデカと
                    st.success(f"💰 【推奨買い目】\n\n### **{race['recommended_bet']}**")
                    
                    # イチオシ馬
                    top_horse = race['top_pickup']
                    st.info(f"🏆 【今週のイチオシ馬】\n\n### **馬番{top_horse['horse_number']}  {top_horse['horse_name']}** (騎手: {top_horse['jockey_name']})")
                    
                    # 予測の詳細
                    with st.expander("📊 AI予想の詳細ランキング（トップ3）"):
                        for j, h in enumerate(race['top_3_horses']):
                            rank_medal = ["🥇", "🥈", "🥉"][j]
                            st.markdown(f"**{rank_medal} 第{j+1}位**: 馬番{h['horse_number']} **{h['horse_name']}** (総合スコア: {h['total_score']:.1f})")
elif password != "":
    st.error("パスワードが違います。")
