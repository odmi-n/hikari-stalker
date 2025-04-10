import os
from dotenv import load_dotenv

# .envを読み込む（ローカル実行時のみ必要）
load_dotenv()

# === 共通設定 ===
EDINET_CODE = os.getenv("EDINET_CODE", "E35239")  # 光通信のコード
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/edinet_downloads")

# LINE API関連
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_USER_ID = os.getenv("LINE_USER_ID")
