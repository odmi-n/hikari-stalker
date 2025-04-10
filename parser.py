import os
import zipfile
import logging
from pathlib import Path
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('edinet_unzipper')

# main.pyから呼び出し可能な関数
def parse_and_filter_reports(download_dir):
    """
    ダウンロードしたデータを解析し、LINE通知用のメッセージリストを生成
    Args:
        download_dir: ダウンロードディレクトリのパス
    Returns:
        list: LINE通知用メッセージのリスト
    """
    # パス解決
    target_dir = Path(download_dir)
    if not target_dir.exists():
        logger.error(f"指定されたディレクトリが存在しません: {target_dir}")
        return []
    
    # パーサーの初期化
    parser = EdinetParser(target_dir)
    
    # ディレクトリ内のデータを解析
    all_results, new_results = parser.parse_directory()
    
    # 通知用メッセージのリストを生成
    messages = []
    
    # 新規の報告書のみ処理
    if new_results:
        logger.info(f"{len(new_results)}件の新規報告書をメッセージ化します")
        for result in new_results:
            # LINE用のメッセージを生成
            line_message = parser.get_line_message(result)
            messages.append(line_message)
    else:
        logger.info("新規の報告書はありませんでした")
    
    logger.info(f"合計{len(messages)}件のメッセージを生成しました")
    return messages

class EdinetUnzipper:
    def __init__(self, target_dir=None):
        """
        初期化
        Args:
            target_dir (str, optional): 処理対象のディレクトリ。指定がない場合は現在のディレクトリを使用。
        """
        self.target_dir = Path(target_dir) if target_dir else Path.cwd()
        logger.info(f"対象ディレクトリ: {self.target_dir}")

    def find_zip_files(self):
        """
        対象ディレクトリ内のZIPファイルを検索
        Returns:
            list: 見つかったZIPファイルのパスのリスト
        """
        zip_files = list(self.target_dir.glob("*.zip"))
        logger.info(f"ZIPファイルが{len(zip_files)}個見つかりました")
        return zip_files

    def unzip_file(self, zip_path):
        """
        ZIPファイルを解凍し、元のファイルを削除
        Args:
            zip_path (Path): 解凍対象のZIPファイルのパス
        Returns:
            bool: 処理が成功したかどうか
        """
        try:
            # 解凍先のディレクトリ名を作成（ZIPファイル名から.zipを除いたもの）
            extract_dir = zip_path.parent / zip_path.stem
            
            # 解凍処理
            logger.info(f"解凍開始: {zip_path}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            logger.info(f"解凍完了: {extract_dir}")

            # ZIPファイルの削除
            zip_path.unlink()
            logger.info(f"ZIPファイル削除完了: {zip_path}")
            
            return True

        except zipfile.BadZipFile:
            logger.error(f"不正なZIPファイル: {zip_path}")
            return False
        except Exception as e:
            logger.error(f"解凍処理中にエラーが発生: {str(e)}")
            return False

    def process_all_zips(self):
        """
        ディレクトリ内の全ZIPファイルを処理
        Returns:
            tuple: (成功件数, 失敗件数)
        """
        success_count = 0
        failure_count = 0

        zip_files = self.find_zip_files()
        if not zip_files:
            logger.info("処理対象のZIPファイルが見つかりませんでした")
            return success_count, failure_count

        for zip_path in zip_files:
            if self.unzip_file(zip_path):
                success_count += 1
            else:
                failure_count += 1

        logger.info(f"処理完了 - 成功: {success_count}件, 失敗: {failure_count}件")
        return success_count, failure_count

class EdinetParser:
    def __init__(self, base_dir):
        """
        初期化
        Args:
            base_dir (str): 解凍されたファイルが格納されているベースディレクトリ
        """
        self.base_dir = Path(base_dir)
        self.setup_logging()
        
        # SQLiteデータベースを使用
        try:
            from db import ReportDatabase
            self.db = ReportDatabase()
            self.logger.info("SQLiteデータベースに接続しました")
        except ImportError:
            self.logger.warning("db モジュールをインポートできません。JSONファイルを使用します。")
            self.processed_reports_file = Path(base_dir).parent / "processed_reports.json"
            self.processed_reports = self.load_processed_reports()

    def setup_logging(self):
        """ロギングの設定"""
        self.logger = logging.getLogger('edinet_parser')
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def load_processed_reports(self):
        """処理済み報告書の情報を読み込む"""
        if self.processed_reports_file.exists():
            try:
                with open(self.processed_reports_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                self.logger.error(f"処理済み報告書ファイルの読み込みエラー: {self.processed_reports_file}")
                return {}
        else:
            self.logger.info(f"処理済み報告書ファイルが見つかりません。新規作成します: {self.processed_reports_file}")
            return {}

    def save_processed_reports(self):
        """処理済み報告書の情報を保存する"""
        try:
            with open(self.processed_reports_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_reports, f, ensure_ascii=False, indent=2)
            self.logger.info(f"処理済み報告書情報を保存しました: {self.processed_reports_file}")
        except Exception as e:
            self.logger.error(f"処理済み報告書情報の保存中にエラー: {str(e)}")

    def is_already_processed(self, report_info):
        """
        報告書が既に処理済みかどうかを判定
        Args:
            report_info (dict): 報告書情報
        Returns:
            bool: 処理済みかどうか
        """
        # 報告書を一意に識別するキーを生成
        report_id = self._generate_report_id(report_info)
        
        # SQLiteデータベースが使用可能な場合はそれを使用
        if hasattr(self, 'db'):
            return self.db.is_already_processed(report_id)
        else:
            # 従来のJSON方式
            if report_id in self.processed_reports:
                self.logger.info(f"この報告書は既に処理済みです: {report_id}")
                return True
            return False

    def _generate_report_id(self, report_info):
        """
        報告書の一意識別子を生成
        Args:
            report_info (dict): 報告書情報
        Returns:
            str: 報告書ID
        """
        # 企業コード、提出日、報告義務発生日、報告書種類、保有者を組み合わせてユニークなIDを生成
        security_code = report_info.get('security_code', '')
        submission_date = report_info.get('submission_date', '')
        report_date = report_info.get('report_date', '')  # 報告義務発生日を追加
        report_type = report_info.get('report_type', '')
        holder_name = report_info.get('holder_name', '')
        
        # 日本語の日付から数字のみを抽出
        submission_numbers = re.sub(r'[^0-9]', '', submission_date)
        report_numbers = re.sub(r'[^0-9]', '', report_date)  # 報告義務発生日の数字を抽出
        
        return f"{security_code}_{submission_numbers}_{report_numbers}_{report_type}_{holder_name}"

    def mark_as_processed(self, report_info):
        """
        報告書を処理済みとしてマーク
        Args:
            report_info (dict): 報告書情報
        """
        # SQLiteデータベースが使用可能な場合はそれを使用
        if hasattr(self, 'db'):
            # report_idを明示的に追加
            report_info['report_id'] = self._generate_report_id(report_info)
            self.db.mark_as_processed(report_info)
        else:
            # 従来のJSON方式
            report_id = self._generate_report_id(report_info)
            
            # 処理日時を含めて保存
            self.processed_reports[report_id] = {
                'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'target_company': report_info.get('target_company', '不明'),
                'security_code': report_info.get('security_code', '不明'),
                'report_type': report_info.get('report_type', '不明'),
                'holder_name': report_info.get('holder_name', '不明'),
                'report_date': report_info.get('report_date', '不明'),  # 報告義務発生日を追加
                'submission_date': report_info.get('submission_date', '不明')  # 提出日も保存
            }
            
            # 変更を保存
            self.save_processed_reports()

    def find_latest_directories(self):
        """
        最新のダウンロードディレクトリを特定する
        Returns:
            list: 最新のディレクトリパスのリスト（最新順）
        """
        # 基本ディレクトリ内のすべてのサブディレクトリを取得
        all_dirs = [d for d in self.base_dir.iterdir() if d.is_dir()]
        
        # 作成日時でソート（最新のものが先頭）
        latest_dirs = sorted(all_dirs, key=lambda d: d.stat().st_mtime, reverse=True)
        
        self.logger.info(f"最新のディレクトリを特定しました: {[d.name for d in latest_dirs[:3]]}")
        return latest_dirs

    def find_all_public_docs(self):
        """
        すべてのPublicDocディレクトリを再帰的に検索
        Returns:
            list: 見つかったPublicDocディレクトリのパスのリスト
        """
        # **/ は再帰的な検索を意味する
        public_docs = list(self.base_dir.glob('**/PublicDoc'))
        self.logger.info(f"{len(public_docs)}個のPublicDocディレクトリを検出しました")
        return public_docs

    def parse_directory(self, specific_dir=None):
        """
        ディレクトリ内の全てのXBRLファイルを処理（最新ディレクトリから検索）
        Args:
            specific_dir (str, optional): 特定のディレクトリを指定する場合のパス
        Returns:
            list: 処理結果のリスト
        """
        try:
            results = []
            new_results = []  # 新規の報告書のみを格納
            
            if specific_dir:
                # 特定のディレクトリが指定された場合
                target_dir = Path(self.base_dir) / specific_dir if not Path(specific_dir).is_absolute() else Path(specific_dir)
                if target_dir.exists():
                    self.logger.info(f"指定されたディレクトリを処理中: {target_dir}")
                    dirs_to_process = [target_dir]
                else:
                    self.logger.error(f"指定されたディレクトリが存在しません: {target_dir}")
                    return results, new_results
            else:
                # 指定がない場合は最新のディレクトリを処理
                dirs_to_process = self.find_latest_directories()
                
                if not dirs_to_process:
                    self.logger.warning("処理対象のディレクトリが見つかりませんでした")
                    return results, new_results
            
            # 各ディレクトリ内のPublicDocディレクトリを検索
            for dir_path in dirs_to_process:
                self.logger.info(f"ディレクトリを処理中: {dir_path.name}")
                
                # PublicDocディレクトリを検索
                for public_doc in dir_path.glob('**/PublicDoc'):
                    self.logger.info(f"PublicDocディレクトリを処理中: {public_doc}")
                    
                    # ヘッダーファイルと本文ファイルを探す
                    header_files = list(public_doc.glob('*header*.htm*'))
                    honbun_files = list(public_doc.glob('*honbun*.htm*'))
                    
                    if header_files and honbun_files:
                        self.logger.info(f"ヘッダーファイル: {header_files[0].name}")
                        self.logger.info(f"本文ファイル: {honbun_files[0].name}")
                        
                        # 各ファイルの最初のものを使用
                        result = self.parse_files(header_files[0], honbun_files[0])
                        if result:
                            # 処理済みかどうかをチェック
                            if not self.is_already_processed(result):
                                # 未処理の報告書を新規リストに追加
                                new_results.append(result)
                                # 処理済みとしてマーク
                                self.mark_as_processed(result)
                            
                            # すべての結果を全体リストに追加（統計用）
                            results.append(result)
                            self.logger.info(f"報告書を処理しました: {result['report_type']} - {result.get('target_company', '不明')}")
            
            self.logger.info(f"合計{len(results)}件の報告書を処理し、うち{len(new_results)}件が新規報告書です")
            
            # 結果を返す前にデータベース接続を閉じる
            if hasattr(self, 'db') and hasattr(self, 'logger'):
                self.logger.info("処理完了後、データベース接続を閉じます")
                self.db.close()
            
            return results, new_results
        except Exception as e:
            self.logger.error(f"ディレクトリ処理中にエラー: {str(e)}")
            # エラー時もデータベース接続を閉じる
            if hasattr(self, 'db'):
                self.db.close()
            return [], []

    def parse_files(self, header_file, honbun_file):
        """
        ヘッダーファイルと本文ファイルを解析
        Args:
            header_file (Path): ヘッダーファイルのパス
            honbun_file (Path): 本文ファイルのパス
        Returns:
            dict: 解析結果
        """
        try:
            # ヘッダーファイルを解析して報告書の種類を判定
            with open(header_file, 'r', encoding='utf-8') as f:
                header_soup = BeautifulSoup(f, 'html.parser')
                report_type = self._get_report_type(header_soup)

            # 本文ファイルを解析
            with open(honbun_file, 'r', encoding='utf-8') as f:
                honbun_soup = BeautifulSoup(f, 'html.parser')

            if report_type == "大量保有報告書":
                return self._parse_large_volume_report(header_soup, honbun_soup)
            elif report_type == "変更報告書":
                return self._parse_change_report(header_soup, honbun_soup)
            else:
                self.logger.warning(f"未対応の報告書タイプ: {report_type}")
                return None

        except Exception as e:
            self.logger.error(f"ファイル解析中にエラーが発生: {str(e)}")
            return None

    def _get_report_type(self, soup):
        """報告書の種類を判定"""
        try:
            # テーブルを検索して「提出書類」欄を探す
            table = soup.find('table')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        # 「提出書類」欄を見つけた場合
                        if "提出書類" in cells[0].text:
                            document_type = cells[1].text.strip()
                            self.logger.info(f"提出書類の種類: {document_type}")
                            
                            # 「変更報告書」という文字が含まれていれば変更報告書
                            if "変更報告書" in document_type:
                                return "変更報告書"
                            # それ以外は大量保有報告書（または他の種類）
                            else:
                                return "大量保有報告書"
            
            # 提出書類欄が見つからなかった場合はタイトルで判断（後方互換性のため）
            title = soup.find('title').text
            if "大量保有報告書" in title and "変更報告書" not in title:
                return "大量保有報告書"
            elif "変更報告書" in title:
                return "変更報告書"
            
            # 判定できない場合
            self.logger.warning("報告書種類の判定ができませんでした。デフォルトで「大量保有報告書」として処理します。")
            return "大量保有報告書"
        except Exception as e:
            self.logger.error(f"報告書種類の判定中にエラー: {str(e)}")
            return "不明"

    def _parse_large_volume_report(self, header_soup, honbun_soup):
        """大量保有報告書の解析"""
        try:
            # 提出者の情報を取得
            filer_info = self._get_filer_info(header_soup)
            
            # 本文から情報を抽出
            data = {
                "report_type": "大量保有報告書",
                "target_company": self._get_text_by_id(honbun_soup, "T0100000000101"),
                "security_code": self._get_text_by_id(honbun_soup, "T0100000000201"),
                "holder_name": self._get_text_by_id(honbun_soup, "T0201010100401") or filer_info.get("name"),
                "holding_ratio": self._get_text_by_id(honbun_soup, "T0201040200201"),
                "report_date": filer_info.get("report_date"),
                "submission_date": filer_info.get("submission_date"),
                "shares_held": self._get_text_by_id(honbun_soup, "T0201040101401"),
                "purpose": self._get_text_by_id(honbun_soup, "T0201020000101")
            }
            
            # データのクリーニング
            data = self._clean_data(data)
            
            return data
        except Exception as e:
            self.logger.error(f"大量保有報告書の解析中にエラー: {str(e)}")
            return None

    def _parse_change_report(self, header_soup, honbun_soup):
        """変更報告書の解析"""
        try:
            # 提出者の情報を取得
            filer_info = self._get_filer_info(header_soup)
            
            # 本文から情報を抽出
            data = {
                "report_type": "変更報告書",
                "target_company": self._get_text_by_id(honbun_soup, "T0100000000101"),
                "security_code": self._get_text_by_id(honbun_soup, "T0100000000201"),
                "holder_name": self._get_text_by_id(honbun_soup, "T0201010100401") or filer_info.get("name"),
                "holding_ratio_before": self._get_text_by_id(honbun_soup, "T0201040200301"),
                "holding_ratio_after": self._get_text_by_id(honbun_soup, "T0201040200201"),
                "report_date": filer_info.get("report_date"),
                "submission_date": filer_info.get("submission_date"),
                "shares_held": self._get_text_by_id(honbun_soup, "T0201040101401"),
                "purpose": self._get_text_by_id(honbun_soup, "T0201020000101")
            }
            
            # データのクリーニング
            data = self._clean_data(data)
            
            return data
        except Exception as e:
            self.logger.error(f"変更報告書の解析中にエラー: {str(e)}")
            return None

    def _get_filer_info(self, header_soup):
        """ヘッダーから提出者情報を取得"""
        info = {}
        
        # テーブルを取得
        table = header_soup.find('table')
        if table:
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    item_cell = cells[0].text.strip()
                    value_cell = cells[1].text.strip()
                    
                    if "氏名又は名称" in item_cell:
                        info["name"] = value_cell
                    elif "報告義務発生日" in item_cell:
                        info["report_date"] = value_cell
                    elif "提出日" in item_cell:
                        info["submission_date"] = value_cell
        
        return info

    def _get_text_by_id(self, soup, id_value):
        """指定されたIDを持つ要素のテキストを取得"""
        element = soup.find(id=id_value)
        return element.text.strip() if element else None

    def _clean_data(self, data):
        """データの整形と数値の抽出"""
        cleaned_data = {}
        
        for key, value in data.items():
            if value is None:
                cleaned_data[key] = None
                continue
                
            # 保有割合から数値を抽出
            if "holding_ratio" in key and value:
                # 数値を抽出 (例: "5.31%" -> "5.31")
                match = re.search(r'(\d+\.\d+|\d+)', value)
                if match:
                    cleaned_data[key] = match.group(1)
                else:
                    cleaned_data[key] = value
            else:
                cleaned_data[key] = value
                
        return cleaned_data

    def get_formatted_result(self, result):
        """結果を整形して表示用のテキストを生成"""
        if not result:
            return "結果がありません"
            
        if result["report_type"] == "大量保有報告書":
            text = f"【大量保有報告書】\n"
            text += f"対象企業: {result.get('target_company', '不明')} ({result.get('security_code', '不明')})\n"
            text += f"保有者: {result.get('holder_name', '不明')}\n"
            text += f"保有割合: {result.get('holding_ratio', '不明')}%\n"
            text += f"保有株式数: {result.get('shares_held', '不明')}株\n"
            text += f"報告義務発生日: {result.get('report_date', '不明')}\n"
            text += f"提出日: {result.get('submission_date', '不明')}\n"
            text += f"目的: {result.get('purpose', '不明')}"
        else:
            text = f"【変更報告書】\n"
            text += f"対象企業: {result.get('target_company', '不明')} ({result.get('security_code', '不明')})\n"
            text += f"保有者: {result.get('holder_name', '不明')}\n"
            text += f"変更前保有割合: {result.get('holding_ratio_before', '不明')}%\n"
            text += f"変更後保有割合: {result.get('holding_ratio_after', '不明')}%\n"
            text += f"保有株式数: {result.get('shares_held', '不明')}株\n"
            text += f"報告義務発生日: {result.get('report_date', '不明')}\n"
            text += f"提出日: {result.get('submission_date', '不明')}\n"
            text += f"目的: {result.get('purpose', '不明')}"
            
        return text

    def get_line_message(self, result):
        """
        LINE用のメッセージを作成
        Args:
            result (dict): 解析結果
        Returns:
            str: LINE用のフォーマットされたメッセージ
        """
        if not result:
            return "結果がありません"
        
        # 絵文字を追加したより見やすいフォーマットで作成
        if result["report_type"] == "大量保有報告書":
            message = f"📊 大量保有報告書\n\n"
            message += f"🏢 {result.get('target_company', '不明')} ({result.get('security_code', '不明')})\n"
            message += f"👤 {result.get('holder_name', '不明')}\n"
            message += f"📈 保有割合: {result.get('holding_ratio', '不明')}%\n"
            message += f"📝 {result.get('shares_held', '不明')}株\n"
            message += f"📅 {result.get('report_date', '不明')}\n"
            message += f"🔍 目的: {result.get('purpose', '不明')}"
        else:
            # 変更前後の割合の差を計算
            before = float(result.get('holding_ratio_before', '0').replace('%', '')) if result.get('holding_ratio_before') else 0
            after = float(result.get('holding_ratio_after', '0').replace('%', '')) if result.get('holding_ratio_after') else 0
            diff = after - before
            diff_str = f"+{diff:.2f}%" if diff > 0 else f"{diff:.2f}%"
            
            message = f"📊 変更報告書\n\n"
            message += f"🏢 {result.get('target_company', '不明')} ({result.get('security_code', '不明')})\n"
            message += f"👤 {result.get('holder_name', '不明')}\n"
            message += f"📉 変更前: {result.get('holding_ratio_before', '不明')}%\n"
            message += f"📈 変更後: {result.get('holding_ratio_after', '不明')}% ({diff_str})\n"
            message += f"📝 {result.get('shares_held', '不明')}株\n"
            message += f"📅 {result.get('report_date', '不明')}\n"
            message += f"🔍 目的: {result.get('purpose', '不明')}"
        
        return message

def main():
    """
    メイン処理
    """
    # EDINETのダウンロードディレクトリを指定（hikariget.pyと同じ場所を想定）
    current_dir = Path.cwd()
    target_dir = current_dir / "edinet_downloads"

    if not target_dir.exists():
        logger.error(f"指定されたディレクトリが存在しません: {target_dir}")
        return

    # コマンドライン引数を処理する場合
    import sys
    specific_dir = None
    
    if len(sys.argv) > 1:
        # 第1引数が指定されている場合は特定のディレクトリとして扱う
        specific_dir = sys.argv[1]
        logger.info(f"特定のディレクトリが指定されました: {specific_dir}")

    # まずZIPファイルの解凍処理を行う
    unzipper = EdinetUnzipper(target_dir)
    success, failure = unzipper.process_all_zips()

    if success + failure > 0:
        logger.info("全てのZIP解凍処理が完了しました")
    else:
        logger.info("処理対象のZIPファイルがありませんでした")

    # パーサーによる解析処理
    parser = EdinetParser(target_dir)
    all_results, new_results = parser.parse_directory(specific_dir)

    # 全ての結果の表示（統計情報用）
    if all_results:
        logger.info(f"合計{len(all_results)}件の報告書を処理し、うち{len(new_results)}件が新規報告書です")
        
        # 新規報告書のみを処理（LINE通知など）
        if new_results:
            logger.info(f"以下の{len(new_results)}件の新規報告書を通知します")
            
            # 通知用にnotifier.pyをインポート
            try:
                from notifier import send_message, send_line_message
                
                for result in new_results:
                    formatted_text = parser.get_formatted_result(result)
                    print(formatted_text)
                    print("---")
                    
                    # LINE用のメッセージを生成
                    line_message = parser.get_line_message(result)
                    logger.info(f"LINE用のメッセージを生成しました:\n{line_message}")
                    
                    # LINEに送信
                    send_message(line_message)
                    logger.info("LINE通知を送信しました")
                
            except ImportError as e:
                logger.error(f"notifierモジュールのインポートに失敗しました: {e}")
                logger.error("LINE通知機能は使用できません")
        else:
            logger.info("新規の報告書はありませんでした")
    else:
        logger.info("処理された報告書はありませんでした")

if __name__ == "__main__":
    main() 