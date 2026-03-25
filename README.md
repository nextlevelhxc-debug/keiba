# keiba_prediction_system

プロジェクトルートディレクトリ
├── data/           # SQLiteデータベースファイルや一時データを保存するディレクトリ
├── src/            # ソースコードディレクトリ
│   ├── config.yaml # 全体設定ファイル
│   ├── database.py # DB接続・テーブル定義（SQLite）
│   ├── ingest/     # フェーズ2: データ収集用スクリプト群
│   ├── features/   # フェーズ3: 特徴量生成スクリプト群
│   ├── models/     # フェーズ4: モデル学習・推論スクリプト群
│   └── strategy/   # フェーズ5-7: 買い目生成、バックテスト、当日予想スクリプト群
└── requirements.txt
