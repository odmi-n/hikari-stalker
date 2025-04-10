import requests
import pandas as pd

# APIのエンドポイント
url = 'https://disclosure.edinet-fsa.go.jp/api/v2/documents.json'

# パラメータの設定（例: 2024年5月17日の書類を取得）
params = {
    'date': '2024-05-17',
    'type': 2,  # 2は有価証券報告書などの決算書類
    "Subscription-Key":"785f614548af4828a614b8da5c39c6a5"
}

# APIリクエストを送信
response = requests.get(url, params=params)

# レスポンスのJSONデータを取得
data = response.json()
data