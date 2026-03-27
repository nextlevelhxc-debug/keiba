#!/bin/bash
# ==============================================================
# 競馬予想システム 一括起動スクリプト
# 使い方: bash start.sh
# ==============================================================

# スクリプトのあるディレクトリに移動
cd "$(dirname "$0")"

echo ""
echo "🐴 競馬予想システムを起動します..."
echo "=================================================="

# 1. 予想データを生成
echo ""
echo "📊 Step 1: 今週末のレース予想を生成中..."
python3 -m src.auto_batch_predict
if [ $? -ne 0 ]; then
    echo "⚠️ 予想生成でエラーが発生しました。続行します..."
fi

# 2. Streamlitアプリをバックグラウンドで起動
echo ""
echo "🌐 Step 2: Webアプリを起動中 (ポート8501)..."
# 既に起動中のStreamlitがあればポートを解放
lsof -ti:8501 | xargs kill -9 2>/dev/null
streamlit run app.py \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false \
    &
STREAMLIT_PID=$!
echo "   Streamlit起動 (PID: $STREAMLIT_PID)"

# Streamlitの起動を待つ
sleep 3

# 3. ngrokでスマホ公開
echo ""
echo "📱 Step 3: スマホでアクセスできるURLを発行中..."
echo ""
echo "=================================================="
echo "✅ アプリ起動完了！"
echo ""
echo "  📍 PC (ローカル)  → http://localhost:8501"
echo ""
echo "  📱 スマホ用URL は下記のngrok URLをご確認ください"
echo "     (Ctrl+C で終了します)"
echo "=================================================="
echo ""

# ngrokでHTTPトンネル作成（Ctrl+Cで終了するまで待機）
ngrok http 8501
