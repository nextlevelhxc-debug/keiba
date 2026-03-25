#!/bin/bash
# 競馬予想Webアプリ起動用スクリプト

echo "======================================"
echo "🐴 競馬予想Webアプリを起動しています..."
echo "======================================"

# プロジェクトのルートディレクトリに移動
cd "$(dirname "$0")"

# 仮想環境があれば有効化
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Streamlitをバックグラウンドで起動
echo "1. サーバーを立ち上げています..."
streamlit run app.py --server.headless true --server.enableCORS=false --server.enableXsrfProtection=false > streamlit.log 2>&1 &
STREAMLIT_PID=$!

# 少し待つ
sleep 3

# cloudflaredで安定した公開用URLを発行 (Cloudflare Tunnel)
echo "2. スマートフォン用の公開URLを発行しています (Cloudflare)..."
cloudflared tunnel --url http://localhost:8501 > cloudflared.log 2>&1 &
CF_PID=$!

sleep 5

# URLを表示
echo ""
echo "✨ 準備完了しました！✨"
echo "スマートフォンから以下のURLにアクセスしてください："
grep -o 'https://.*trycloudflare.com' cloudflared.log | head -n 1
echo ""
echo "※パスワードは「keiba」です。"
echo "======================================"
echo "（終了するときは、このターミナルウィンドウを閉じてください）"

# 終了待機 (プロセスを維持)
wait
