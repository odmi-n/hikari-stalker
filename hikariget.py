import requests
import os
import zipfile
from io import BytesIO
import time
import logging
import json
import re
from datetime import datetime
from urllib.parse import urlparse, urljoin
from parser import EdinetUnzipper
from config import EDINET_CODE, DOWNLOAD_DIR  # configから設定を使用
from dotenv import load_dotenv

# .envから環境変数を読み込む
load_dotenv()

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('edinet_downloader')

# main.pyから呼び出し可能な関数
def fetch_reports(date_str):
    """
    指定日付の大量保有報告書を検索・ダウンロード
    Args:
        date_str: 検索日付（YYYY-MM-DD形式）
    Returns:
        bool: 処理の成功/失敗
    """
    try:
        downloader = EdinetDownloader()
        
        # APIキーを環境変数から取得
        downloader.api_key = os.getenv("EDINET_API_KEY")
        if not downloader.api_key:
            logger.warning("EDINET_API_KEY が環境変数に設定されていません")
        
        # URLの探索
        downloader.discover_actual_urls()
        
        # 書類のダウンロード
        logger.info(f"{date_str}の大量保有報告書を検索・ダウンロードします")
        successful_downloads = downloader.find_and_download_all_holdings_reports(date_str)
        
        if successful_downloads:
            logger.info(f"{len(successful_downloads)}件のファイルをダウンロードしました")
            
            # ZIPファイルの解凍処理
            unzipper = EdinetUnzipper(downloader.save_dir)
            success, failure = unzipper.process_all_zips()
            logger.info(f"解凍処理完了 - 成功: {success}件, 失敗: {failure}件")
            return True
        else:
            logger.info(f"{date_str}に該当する書類はありませんでした")
            return False
    
    except Exception as e:
        logger.error(f"報告書の取得処理中にエラーが発生しました: {e}")
        return False

class EdinetDownloader:
    # EDINETの初期URLとAPI関連
    BASE_URL = "https://disclosure.edinet-fsa.go.jp"
    API_ENDPOINT_TEMPLATE = "{base_url}/api/v2/documents.json"
    
    # 詳細なブラウザのUser-Agent
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Origin": None,  # 動的に設定
        "Referer": None,  # 動的に設定
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Connection": "keep-alive",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.actual_base_url = None  # 実際に使用するベースURL（リダイレクト後）
        self.api_endpoint = None  # 実際に使用するAPIエンドポイント
        self.api_key = None  # APIキーを格納
        
        # 保存ディレクトリの作成
        self.save_dir = DOWNLOAD_DIR
        os.makedirs(self.save_dir, exist_ok=True)
        
        # デバッグ用のログディレクトリ
        self.log_dir = os.path.join(self.save_dir, "logs")
        os.makedirs(self.log_dir, exist_ok=True)
    
    def save_debug_info(self, name, content, is_binary=False):
        """デバッグ情報をファイルに保存"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        mode = 'wb' if is_binary else 'w'
        encoding = None if is_binary else 'utf-8'
        
        filename = f"{timestamp}_{name}"
        filepath = os.path.join(self.log_dir, filename)
        
        try:
            with open(filepath, mode, encoding=encoding) as f:
                f.write(content)
            logger.debug(f"デバッグ情報を保存: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"デバッグ情報保存エラー: {str(e)}")
            return None
    
    def get_api_key(self):
        """APIキーを取得"""
        # 環境変数からAPIキーを取得
        api_key = os.environ.get("EDINET_API_KEY")
        
        # 環境変数に無い場合はユーザーに入力を促す
        if not api_key:
            api_key = input("EDINETのAPIキーを入力してください: ")
            
        # APIキーの形式を簡易的に検証
        if api_key and len(api_key) > 10:  # 単純な長さチェック
            logger.info("APIキーを設定しました")
            return api_key
        else:
            logger.warning("有効なAPIキーが見つかりません")
            retry = input("APIキーなしで続行しますか？(y/n): ")
            if retry.lower() == 'y':
                logger.warning("APIキーなしで続行します（機能が制限される可能性があります）")
                return ""
            else:
                logger.error("有効なAPIキーが必要です。プログラムを終了します。")
                exit(1)
    
    def get_target_date(self):
        """ユーザーから日付を取得"""
        while True:
            date_input = input("日付を入力してください（例：2025-04-10）：")
            date_input = date_input.strip()
            
            # 日付形式の検証
            try:
                datetime.strptime(date_input, '%Y-%m-%d')
                return date_input
            except ValueError:
                logger.error("無効な日付形式です。YYYY-MM-DDの形式で入力してください。")
    
    def discover_actual_urls(self):
        """EDINETサイトにアクセスして実際のURLとAPIエンドポイントを発見する"""
        try:
            # 初期アクセスとリダイレクト追跡
            logger.info(f"EDINETサイトの探索を開始: {self.BASE_URL}")
            
            response = self.session.get(
                self.BASE_URL, 
                allow_redirects=True,
                timeout=30
            )
            
            # 最終的なURLを取得（リダイレクト後）
            final_url = response.url
            parsed_url = urlparse(final_url)
            self.actual_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            logger.info(f"リダイレクト後の実際のベースURL: {self.actual_base_url}")
            
            # APIエンドポイントを更新
            self.api_endpoint = self.API_ENDPOINT_TEMPLATE.format(base_url=self.actual_base_url)
            logger.info(f"使用するAPIエンドポイント: {self.api_endpoint}")
            
            # レスポンス内容を保存
            self.save_debug_info("edinet_main_page.html", response.text)
            
            # ページ内からJavaScriptのURLを探して訪問（セッション確立のため）
            js_urls = re.findall(r'src="(/[^"]+\.js)"', response.text)
            if js_urls:
                for js_url in js_urls[:2]:  # 最初の2つだけ取得
                    full_js_url = urljoin(self.actual_base_url, js_url)
                    logger.info(f"JavaScriptファイルにアクセス: {full_js_url}")
                    js_response = self.session.get(full_js_url, timeout=10)
                    if js_response.status_code == 200:
                        logger.debug(f"JavaScriptファイル取得成功: {full_js_url}")
            
            # クッキー情報を出力
            cookies = self.session.cookies.get_dict()
            logger.info(f"取得したクッキー: {cookies}")
            
            # ヘッダーの更新
            self.HEADERS["Origin"] = self.actual_base_url
            self.HEADERS["Referer"] = final_url
            
            # ヘッダーを設定
            for key, value in self.HEADERS.items():
                if value is not None:
                    self.session.headers[key] = value
            
            return True
            
        except Exception as e:
            logger.error(f"URL探索中にエラー: {str(e)}")
            return False
    
    def get_documents_list(self, date_str):
        """指定日付のEDINET提出書類一覧を取得"""
        try:
            # 直接APIエンドポイントを使用
            endpoint = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
            
            params = {
                "date": date_str,
                "type": 2  # 提出書類一覧APIの種別（2: メタデータのみ）
            }
            
            # APIキーをSubscription-Keyパラメータとして設定
            if self.api_key:
                params["Subscription-Key"] = self.api_key
            
            logger.info(f"書類リスト取得URL: {endpoint}?date={date_str}&type=2")
            logger.info("APIリクエスト前に待機中...")
            time.sleep(3)
            
            # シンプルなヘッダーを使用
            headers = {
                "Accept": "application/json"
            }
            
            response = requests.get(endpoint, params=params, headers=headers, timeout=30)
            logger.info(f"書類リストレスポンス: HTTP {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # フィルタリング前のデータ保存は開発時のみ
                    # コメントアウトしておく（必要時に復活）
                    # self.save_debug_info(f"documents_list_{date_str.replace('-', '')}_full.json", 
                    #                     json.dumps(data, indent=2, ensure_ascii=False))
                    
                    if data.get("metadata", {}).get("status") == "200":
                        count = data.get("metadata", {}).get("resultset", {}).get("count", 0)
                        logger.info(f"書類リスト取得成功: {date_str}の書類数 {count}")
                        
                        # 光通信関連の書類だけをフィルタリングして別名で保存
                        results = data.get("results", [])
                        kotsu_docs = self.filter_only_kotsu_documents(results)
                        
                        # 光通信の書類だけを保存
                        if kotsu_docs:
                            filtered_data = {
                                "metadata": data.get("metadata", {}),
                                "results": kotsu_docs
                            }
                            self.save_debug_info(f"kotsu_documents_{date_str.replace('-', '')}.json", 
                                                json.dumps(filtered_data, indent=2, ensure_ascii=False))
                            logger.info(f"光通信の書類のみ {len(kotsu_docs)}件 を保存しました")
                        else:
                            logger.info("光通信の書類は見つかりませんでした")
                        
                        return results  # 全データを返す（後続の詳細なフィルタリング用）
                    else:
                        message = data.get("metadata", {}).get("message", "不明なエラー")
                        logger.error(f"APIエラー: {message}")
                except json.JSONDecodeError:
                    logger.error("レスポンスがJSON形式ではありません")
                    self.save_debug_info(f"invalid_json_response_{date_str}.txt", response.text)
            else:
                self.save_debug_info(f"documents_list_{date_str.replace('-', '')}_error.txt", response.text)
                logger.error(f"書類リスト取得失敗（HTTP {response.status_code}）")
                
                # APIキーがない場合はキーが必要な可能性を示唆
                if response.status_code == 403 and not self.api_key:
                    logger.error("アクセス拒否（403）: APIキーが必要な可能性があります")
                    logger.info("APIキーを取得して再試行することを検討してください")
                    
                # APIキーがあるのに403の場合はキーが無効の可能性
                elif response.status_code == 403 and self.api_key:
                    logger.error("アクセス拒否（403）: APIキーが無効か期限切れの可能性があります")
                    logger.info("新しいAPIキーを取得するか、アクセス権限を確認してください")
            
            return []
            
        except Exception as e:
            logger.error(f"リクエスト中にエラーが発生: {str(e)}")
            return []
    
    def filter_only_kotsu_documents(self, documents):
        """対象の企業（光通信）に関連する書類のみをフィルタリング"""
        filtered_docs = []
        for doc in documents:
            if doc.get('edinetCode') == EDINET_CODE:
                filtered_docs.append(doc)
                
        logger.info(f"光通信関連書類: {len(filtered_docs)}件")
        return filtered_docs
    
    def download_document(self, doc_id):
        """指定docIDの書類をZIPでダウンロード・解凍"""
        try:
            endpoint = f"https://disclosure.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
            params = {
                "type": 1  # 書類取得APIの種別（1: 提出本文のPDFのZIP）
            }
            
            # APIキーをSubscription-Keyパラメータとして設定
            if self.api_key:
                params["Subscription-Key"] = self.api_key
            
            # シンプルなヘッダー
            headers = {
                "Accept": "application/octet-stream"
            }
            
            logger.info(f"書類 {doc_id} のダウンロードを開始...")
            time.sleep(3)
            
            response = requests.get(endpoint, params=params, headers=headers, stream=True, timeout=60)
            logger.info(f"ダウンロードレスポンス: HTTP {response.status_code}")
            
            if response.status_code == 200:
                # ファイル保存用のディレクトリ
                doc_dir = os.path.join(self.save_dir, doc_id)
                os.makedirs(doc_dir, exist_ok=True)
                
                # ZIPファイルとして保存
                zip_path = os.path.join(self.save_dir, f"{doc_id}.zip")
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"ダウンロード完了: {zip_path} ({os.path.getsize(zip_path)} bytes)")
                
                # ZIPファイルを解凍
                try:
                    with zipfile.ZipFile(zip_path) as z:
                        z.extractall(doc_dir)
                    logger.info(f"[成功] {doc_id} をダウンロード・展開しました。保存先: {doc_dir}")
                    return True
                except zipfile.BadZipFile:
                    logger.error("ダウンロードしたファイルはZIPファイルではありません")
                    # エラーレスポンスを保存
                    self.save_debug_info(f"{doc_id}_not_zip.txt", response.content[:1000])
            else:
                logger.error(f"{doc_id} のダウンロード失敗（HTTP {response.status_code}）")
                # エラーレスポンスを保存
                self.save_debug_info(f"{doc_id}_download_error.txt", 
                                    response.text if len(response.content) < 10000 else "レスポンスが大きすぎるため省略")
            
            return False
            
        except Exception as e:
            logger.error(f"ダウンロード中にエラーが発生: {str(e)}")
            return False
    
    def filter_documents(self, documents):
        """書類リストから目的の書類をフィルタリング"""
        filtered_docs = []
        
        # 処理前のデバッグ情報
        logger.info(f"フィルタリング前の書類数: {len(documents)}")
        
        # 最初の数件のドキュメントの構造を確認
        for i, doc in enumerate(documents[:3]):
            logger.debug(f"ドキュメント構造サンプル {i+1}: {json.dumps(doc, ensure_ascii=False)}")
        
        # 光通信の書類をフィルタリング (すべての種類の書類を対象)
        for doc in documents:
            edinetCode = doc.get("edinetCode", "")
            secCode = doc.get("secCode", "")
            filerName = doc.get("filerName", "")  # デフォルト値を空文字列に設定
            formCode = doc.get("formCode", "")
            docDescription = doc.get("docDescription", "")
            
            # 光通信の書類を検出 (複数の条件で確認)
            is_kotsu = False
            
            # EDINETコードで判定
            if edinetCode == EDINET_CODE:
                is_kotsu = True
                logger.info(f"EDINETコードで光通信の書類を検出: {docDescription}")
            
            # 発行者名に「光通信」が含まれる場合も対象に
            elif filerName and "光通信" in filerName:  # filerNameがNoneでないことを確認
                is_kotsu = True
                logger.info(f"発行者名で光通信の書類を検出: {filerName} - {docDescription}")
            
            if is_kotsu:
                logger.info(f"光通信の書類を追加: {docDescription} (ID: {doc.get('docID')}, フォームコード: {formCode})")
                filtered_docs.append(doc)
        
        logger.info(f"フィルタリング後の光通信書類数: {len(filtered_docs)}")
        return filtered_docs
    
    def find_and_download_all_holdings_reports(self, date_str):
        """特定日付の全ての大量保有報告書を検索・ダウンロード"""
        # 書類リスト取得
        documents = self.get_documents_list(date_str)
        
        if not documents:
            logger.warning(f"{date_str}の書類リストが取得できないか、書類がありません")
            return []
        
        # 書類数を表示
        logger.info(f"取得した書類リスト: {len(documents)}件")
        
        # 光通信の大量保有報告書をフィルタリング
        target_docs = self.filter_documents(documents)
        
        if not target_docs:
            logger.info(f"{date_str}に光通信の大量保有報告書の提出はありませんでした")
            return []
        
        # 検出した書類を表示
        logger.info(f"光通信の大量保有報告書: {len(target_docs)}件")
        
        # 書類をダウンロード
        successful_downloads = []
        for doc in target_docs:
            doc_id = doc.get("docID")
            doc_description = doc.get("docDescription", "大量保有報告書")
            
            logger.info(f"{doc_description} ({doc_id}) をダウンロードします...")
            if self.download_document(doc_id):
                successful_downloads.append(doc_id)
        
        return successful_downloads
        
    def run(self):
        """メイン処理"""
        logger.info("EDINETダウンローダーを開始します (APIキー対応版)")
        
        # APIキーを取得
        self.api_key = self.get_api_key()
        
        # 環境情報を出力
        logger.info(f"Python Requests バージョン: {requests.__version__}")
        logger.info(f"実行ディレクトリ: {os.getcwd()}")
        
        # 実際のURLを発見
        if not self.discover_actual_urls():
            logger.error("EDINETサイトの探索に失敗しました。処理を続行します...")
        
        # ユーザーから日付を取得
        target_date = self.get_target_date()
        logger.info(f"指定された日付: {target_date}")
        
        # 書類検索・ダウンロード実行
        successful_downloads = self.find_and_download_all_holdings_reports(target_date)
        
        # 処理結果のサマリーを表示
        if successful_downloads:
            logger.info(f"処理完了: {len(successful_downloads)}件のファイルをダウンロードしました")
            for i, doc_id in enumerate(successful_downloads, 1):
                doc_dir = os.path.join(self.save_dir, doc_id)
                logger.info(f"{i}. ドキュメントID: {doc_id} - 保存先: {doc_dir}")
            
            # 追加：ダウンロードが成功した場合、自動的に解凍処理を実行
            logger.info("ダウンロードしたZIPファイルの解凍を開始します...")
            unzipper = EdinetUnzipper(self.save_dir)
            success, failure = unzipper.process_all_zips()
            logger.info(f"解凍処理完了 - 成功: {success}件, 失敗: {failure}件")
        else:
            logger.warning("ダウンロードされたファイルはありません。設定やAPIキーを確認してください。")
            
        logger.info("EDINETダウンローダーを終了します")

if __name__ == "__main__":
    downloader = EdinetDownloader()
    downloader.run()