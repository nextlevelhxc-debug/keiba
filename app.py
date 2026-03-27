import streamlit as st
import os
import json

# ページ設定（スマートフォン対応）
st.set_page_config(page_title="競馬予想アプリ", page_icon="🐴", layout="wide")

# カスタムCSS（スマホでも読みやすいサイズに）
st.markdown("""
<style>
    .value-card { 
        background: linear-gradient(135deg, #1a1a2e, #16213e);
        border: 2px solid #f39c12;
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
        color: white;
    }
    .value-score { font-size: 2em; font-weight: bold; color: #f39c12; }
    .race-header { font-size: 1.1em; color: #aaa; }
    .horse-name { font-size: 1.4em; font-weight: bold; color: white; }
</style>
""", unsafe_allow_html=True)

st.title("🐴 競馬予想アプリ (LightGBM版)")

# パスワード認証
password = st.text_input("パスワードを入力してください", type="password")
correct_password = "keiba"

if password == correct_password:
    st.success("認証成功！ようこそ！")
    
    # --- システム管理（メイン画面トップに表示） ---
    with st.expander("🔄 データの更新・操作ガイド", expanded=True):
        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown("""
            **自動更新スケジュール:**
            - 金曜 18:00頃（枠順確定）
            - 土曜 08:30頃（当日オッズ）
            - 日曜 08:30頃（当日オッズ）
            """)
        with col2:
            actions_url = "https://github.com/nextlevelhxc-debug/keiba/actions/workflows/auto_predict.yml"
            st.link_button("🚀 今すぐ更新する", actions_url, use_container_width=True)
        
        st.caption("※GitHubで『Run workflow』を押した後、反映まで3〜5分かかります。")

    st.markdown("---")

    # 予想データの読み込み
    base_dir = os.path.abspath(os.path.dirname(__file__))
    data_file = os.path.join(base_dir, "data", "latest_prediction.json")

    if not os.path.exists(data_file):
        st.warning("⚠️ まだ今週の予想データが作成されていません。")
        st.info("データを生成するには、ターミナルで以下を実行してください:\n```bash\npython3 -m src.auto_batch_predict\n```")
    else:
        with open(data_file, 'r', encoding='utf-8') as f:
            pred_data = json.load(f)

        # 更新ステータスの表示 (SOP準拠)
        st.info(f"📅 更新ステータス: **{pred_data.get('update_status', '【最新予想】')}**")
        st.caption(f"🔄 最終更新日時: {pred_data.get('generated_at', '不明')}")

        # ============================================================
        # 【最上部】今週の妙味ピックアップ（期待値1.2以上）
        # ============================================================
        value_picks = pred_data.get("value_picks", [])
        if value_picks:
            st.markdown("## 🔥 投資価値の高い「妙味馬」 TOP5")
            st.markdown("AI予測の複勝率と市場オッズを比較し、期待値が **1.2以上** の馬を抽出しています。")

            for i, pick in enumerate(value_picks):
                score = pick.get('expectancy_score', 0)
                race_label = pick.get('race_name', f"{pick.get('place', '')} {pick.get('round', '')}R")
                
                with st.container():
                    st.markdown(f"""
<div class="value-card" style="border-left: 10px solid {'#f1c40f' if i==0 else '#e67e22'};">
    <div style="display: flex; justify-content: space-between; align-items: start;">
        <div>
            <div class="race-header">{race_label}</div>
            <div class="horse-name">馬番{int(pick['horse_number'])} {pick['horse_name']}</div>
            <div style="color:#aaa;">騎手: {pick['jockey_name']}</div>
        </div>
        <div style="text-align: right;">
            <div style="color:#f39c12; font-size: 0.9em;">期待値</div>
            <div class="value-score">{score}</div>
        </div>
    </div>
    <div style="margin-top:12px; padding:10px; background:rgba(255,255,255,0.05); border-radius:6px; font-size:0.95em; color:#eee;">
        <b>💡 定量的根拠:</b><br>{pick.get('reasoning', 'AIによる総合力評価')}
    </div>
</div>
""", unsafe_allow_html=True)

            st.markdown("---")

        # ============================================================
        # 各レースの予想（タブ形式）
        # ============================================================
        races = pred_data.get("races", [])
        if not races:
            st.info("🏇 今週末の重賞レースデータはまだ発表されていません。")
        else:
            st.markdown("## 🎯 レース別 詳細分析")
            tab_labels = [f"{r['race_name']}" for r in races]
            tabs = st.tabs(tab_labels)

            for i, race in enumerate(races):
                with tabs[i]:
                    header_suffix = " 🏆(重賞)" if race.get('is_graded') else ""
                    st.header(f"🏟️ {race['race_name']}{header_suffix}")

                    st.markdown("### 🏆 AI特効ピックアップ")
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        top = race['top_pickup']
                        st.info(
                            f"🥇 **【単勝候補】**\n\n"
                            f"### 馬番{int(top['horse_number'])} {top['horse_name']}\n\n"
                            f"予想勝率: **{top.get('win_prob', 0)}%**"
                        )
                    with col2:
                        axis = race['axis_pickup']
                        st.success(
                            f"🎯 **【複勝/軸】**\n\n"
                            f"### 馬番{int(axis['horse_number'])} {axis['horse_name']}\n\n"
                            f"予測複勝率: **{axis.get('place_prob', 0)}%**"
                        )
                    with col3:
                        dark = race['darkhorse_pickup']
                        st.error(
                            f"🔥 **【妙味/穴】**\n\n"
                            f"### 馬番{int(dark['horse_number'])} {dark['horse_name']}\n\n"
                            f"期待値: **{dark.get('expectancy_score', 0)}**"
                        )

                    st.markdown("---")
                    st.warning(f"💰 【推奨買い目】\n\n### **{race['recommended_bet']}**")

                    with st.expander("📊 AI分析 全頭データ"):
                        import pandas as pd
                        df = pd.DataFrame(race['all_horses'])
                        if not df.empty:
                            show_cols = [c for c in ['horse_number', 'horse_name', 'jockey_name',
                                                     'win_prob', 'place_prob', 'expectancy_score',
                                                     'odds', 'popularity', 'reasoning'] if c in df.columns]
                            rename_map = {
                                'horse_number': '馬番', 'horse_name': '馬名', 'jockey_name': '騎手',
                                'win_prob': '勝率(%)', 'place_prob': '複勝率(%)',
                                'expectancy_score': '期待値', 'odds': 'オッズ',
                                'popularity': '人気', 'reasoning': '根拠'
                            }
                            display_df = df[show_cols].rename(columns=rename_map)
                            st.dataframe(display_df, use_container_width=True)

elif password != "":
    st.error("パスワードが違います。")
