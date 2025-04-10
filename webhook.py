from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import os
from dotenv import load_dotenv

# .envファイルの読み込み
load_dotenv()

app = Flask(__name__)

# LINEの認証情報（.envやconfig.pyで安全に管理するのが理想）
CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
LINE_USER_ID = os.getenv("LINE_USER_ID")

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    print("Error: LINE_CHANNEL_ACCESS_TOKEN と LINE_CHANNEL_SECRET の環境変数を設定してください。")
    print("例: export LINE_CHANNEL_ACCESS_TOKEN='your_token'")
    print("例: export LINE_CHANNEL_SECRET='your_secret'")
    exit(1)

if not LINE_USER_ID:
    print("Warning: LINE_USER_ID が設定されていません。プッシュメッセージ機能は利用できません。")
    print("webhook で受信したメッセージのユーザーIDをコピーして .env ファイルに設定してください。")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# ルートページ
@app.route("/", methods=['GET'])
def index():
    html = """
    <h1>LINE Bot API サーバー</h1>
    <p>以下のエンドポイントが利用可能です：</p>
    <ul>
        <li><a href="/send_message?message=こんにちは">GET /send_message?message=メッセージ内容</a> - 設定されたユーザーにプッシュメッセージを送信</li>
        <li>POST /callback - LINE Platformからのwebhookを受け取るエンドポイント</li>
    </ul>
    <p>LINE_USER_ID: {user_id}</p>
    """
    
    return html.format(user_id=LINE_USER_ID if LINE_USER_ID else "未設定")

# プッシュメッセージを送信する関数
def send_push_message(user_id, message):
    try:
        line_bot_api.push_message(user_id, TextSendMessage(text=message))
        return True
    except Exception as e:
        print(f"プッシュメッセージ送信エラー: {e}")
        return False

# プッシュメッセージ送信のエンドポイント
@app.route("/send_message", methods=['GET'])
def send_message():
    if not LINE_USER_ID:
        return "LINE_USER_ID が設定されていません。", 400
    
    message = request.args.get('message', 'テストメッセージです')
    success = send_push_message(LINE_USER_ID, message)
    
    if success:
        return "メッセージを送信しました！", 200
    else:
        return "メッセージの送信に失敗しました。", 500

@app.route("/callback", methods=['POST'])
def callback():
    # 署名検証
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

# イベント受信時の処理
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text
    print(f"✅ ユーザーID: {user_id}")
    print(f"📩 メッセージ内容: {text}")
    
    # LINE_USER_IDが設定されていない場合は、ユーザーIDを教える
    if not LINE_USER_ID:
        reply = TextSendMessage(text=f"あなたのユーザーID: {user_id}\n.envファイルのLINE_USER_IDに設定してください。")
        line_bot_api.reply_message(event.reply_token, reply)
    else:
        # 通常の返信
        reply = TextSendMessage(text=f"あなたが送った内容：{text}")
        line_bot_api.reply_message(event.reply_token, reply)

if __name__ == "__main__":
    app.run(port=5000)
