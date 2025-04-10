from linebot import LineBotApi
from linebot.models import TextSendMessage
import os
from dotenv import load_dotenv

# .envから環境変数を読み込む
load_dotenv()

# 環境変数からトークンとユーザーIDを取得
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_USER_ID = os.getenv("LINE_USER_ID")

# Botの初期化
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)

def send_message(message: str, user_id: str = LINE_USER_ID):
    """
    指定ユーザーにメッセージを送信
    """
    try:
        line_bot_api.push_message(user_id, TextSendMessage(text=message))
        print("✅ メッセージを送信しました")
    except Exception as e:
        print(f"❌ メッセージ送信に失敗しました: {e}")

# ユーザーの提供したコードと互換性を保つためのエイリアス
def send_line_message(message: str):
    """
    指定したLINEユーザーにメッセージを送信する
    """
    try:
        line_bot_api.push_message(
            LINE_USER_ID,
            TextSendMessage(text=message)
        )
        print("✅ LINEメッセージを送信しました。")
    except Exception as e:
        print(f"❌ メッセージ送信に失敗しました: {e}")

# テスト用エントリーポイント（ターミナルから実行可）
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("使い方: python notifier.py 'メッセージ'")
        exit(1)
    test_message = sys.argv[1]
    send_message(test_message)
