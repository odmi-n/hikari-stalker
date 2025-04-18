import sqlite3
import json
import os
import logging
from datetime import datetime
from pathlib import Path

# MySQL接続用のインポート（オプション）
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

# .env ファイルから環境変数を読み込む（オプション）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('edinet_db')

class ReportDatabase:
    def __init__(self, db_path='edinet_reports.db'):
        """
        データベース接続の初期化
        Args:
            db_path: SQLiteデータベースファイルのパス
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """データベースへの接続を確立"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # 行を辞書形式で取得
            self.cursor = self.conn.cursor()
            logger.info(f"データベースに接続しました: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"データベース接続エラー: {e}")
            raise
    
    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            logger.info("データベース接続を閉じました")
    
    def create_tables(self):
        """必要なテーブルを作成"""
        try:
            # 報告書テーブルの作成
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_reports (
                report_id TEXT PRIMARY KEY,
                processed_at TEXT,
                target_company TEXT,
                security_code TEXT,
                report_type TEXT,
                holder_name TEXT,
                report_date TEXT,
                submission_date TEXT
            )
            ''')
            
            # インデックスの作成
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_security_code ON processed_reports (security_code)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_holder_name ON processed_reports (holder_name)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_report_type ON processed_reports (report_type)')
            
            self.conn.commit()
            logger.info("テーブルの作成が完了しました")
        except sqlite3.Error as e:
            logger.error(f"テーブル作成エラー: {e}")
            raise
    
    def import_from_json(self, json_file_path='processed_reports.json'):
        """
        既存のJSONファイルからデータをインポート
        Args:
            json_file_path: インポート元のJSONファイルパス
        Returns:
            int: インポートした件数
        """
        try:
            if not os.path.exists(json_file_path):
                logger.warning(f"JSONファイルが見つかりません: {json_file_path}")
                return 0
            
            with open(json_file_path, 'r', encoding='utf-8') as f:
                reports = json.load(f)
            
            count = 0
            for report_id, report_data in reports.items():
                # データを挿入
                try:
                    self.cursor.execute('''
                    INSERT OR REPLACE INTO processed_reports 
                    (report_id, processed_at, target_company, security_code, 
                    report_type, holder_name, report_date, submission_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        report_id,
                        report_data.get('processed_at'),
                        report_data.get('target_company'),
                        report_data.get('security_code'),
                        report_data.get('report_type'),
                        report_data.get('holder_name'),
                        report_data.get('report_date'),
                        report_data.get('submission_date')
                    ))
                    count += 1
                except sqlite3.Error as e:
                    logger.error(f"レコード {report_id} のインポート中にエラー: {e}")
            
            self.conn.commit()
            logger.info(f"{count}件のレコードをJSONからインポートしました")
            return count
        
        except Exception as e:
            logger.error(f"JSONインポート中にエラー: {e}")
            self.conn.rollback()
            raise
    
    def is_already_processed(self, report_id):
        """
        報告書が既に処理済みかどうかを判定
        Args:
            report_id: 報告書ID
        Returns:
            bool: 処理済みかどうか
        """
        try:
            self.cursor.execute('SELECT 1 FROM processed_reports WHERE report_id = ?', (report_id,))
            result = self.cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            logger.error(f"報告書チェック中にエラー: {e}")
            return False
    
    def mark_as_processed(self, report_info):
        """
        報告書を処理済みとしてマーク
        Args:
            report_info: 報告書情報の辞書
        Returns:
            bool: 処理が成功したかどうか
        """
        try:
            report_id = report_info.get('report_id')
            if not report_id:
                # report_idがない場合は、生成ロジックに従って作成
                security_code = report_info.get('security_code', '')
                submission_date = report_info.get('submission_date', '')
                report_date = report_info.get('report_date', '')
                report_type = report_info.get('report_type', '')
                holder_name = report_info.get('holder_name', '')
                
                # 日本語の日付から数字のみを抽出
                submission_numbers = ''.join(filter(str.isdigit, submission_date))
                report_numbers = ''.join(filter(str.isdigit, report_date))
                
                report_id = f"{security_code}_{submission_numbers}_{report_numbers}_{report_type}_{holder_name}"
            
            # 処理日時
            processed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
            INSERT OR REPLACE INTO processed_reports 
            (report_id, processed_at, target_company, security_code, 
            report_type, holder_name, report_date, submission_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                report_id,
                processed_at,
                report_info.get('target_company', '不明'),
                report_info.get('security_code', '不明'),
                report_info.get('report_type', '不明'),
                report_info.get('holder_name', '不明'),
                report_info.get('report_date', '不明'),
                report_info.get('submission_date', '不明')
            ))
            
            self.conn.commit()
            logger.info(f"報告書 {report_id} を処理済みとして記録しました")
            return True
        
        except sqlite3.Error as e:
            logger.error(f"報告書マーク中にエラー: {e}")
            self.conn.rollback()
            return False
    
    def get_all_processed_reports(self):
        """
        すべての処理済み報告書を取得
        Returns:
            list: 処理済み報告書のリスト
        """
        try:
            self.cursor.execute('SELECT * FROM processed_reports ORDER BY processed_at DESC')
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"処理済み報告書取得中にエラー: {e}")
            return []
    
    def export_to_json(self, json_file_path='processed_reports_export.json'):
        """
        データベースからJSONファイルにエクスポート
        Args:
            json_file_path: エクスポート先のJSONファイルパス
        Returns:
            bool: エクスポートが成功したかどうか
        """
        try:
            reports = self.get_all_processed_reports()
            
            # 辞書形式に変換
            report_dict = {}
            for report in reports:
                report_id = report.pop('report_id')
                report_dict[report_id] = report
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(report_dict, f, ensure_ascii=False, indent=2)
            
            logger.info(f"{len(reports)}件のレコードをJSONにエクスポートしました: {json_file_path}")
            return True
        
        except Exception as e:
            logger.error(f"JSONエクスポート中にエラー: {e}")
            return False
    
    def search_reports(self, 
                       security_code=None, 
                       holder_name=None, 
                       report_type=None,
                       target_company=None,
                       limit=100):
        """
        条件に一致する報告書を検索
        Args:
            security_code: 証券コード
            holder_name: 保有者名
            report_type: 報告書種類
            target_company: 対象企業名
            limit: 取得する最大件数
        Returns:
            list: 一致する報告書のリスト
        """
        try:
            query = 'SELECT * FROM processed_reports WHERE 1=1'
            params = []
            
            if security_code:
                query += ' AND security_code = ?'
                params.append(security_code)
            
            if holder_name:
                query += ' AND holder_name LIKE ?'
                params.append(f'%{holder_name}%')
            
            if report_type:
                query += ' AND report_type = ?'
                params.append(report_type)
            
            if target_company:
                query += ' AND target_company LIKE ?'
                params.append(f'%{target_company}%')
            
            query += ' ORDER BY processed_at DESC LIMIT ?'
            params.append(limit)
            
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        
        except sqlite3.Error as e:
            logger.error(f"報告書検索中にエラー: {e}")
            return []
    
    def get_report_counts_by_type(self):
        """
        報告書種類ごとの件数を取得
        Returns:
            dict: 報告書種類と件数の辞書
        """
        try:
            self.cursor.execute('SELECT report_type, COUNT(*) as count FROM processed_reports GROUP BY report_type')
            rows = self.cursor.fetchall()
            return {row['report_type']: row['count'] for row in rows}
        except sqlite3.Error as e:
            logger.error(f"集計中にエラー: {e}")
            return {}
    
    def get_latest_date_reports(self):
        """
        最新の日付に提出された報告書のみを取得
        Returns:
            list: 最新日付の報告書のリスト
        """
        try:
            # まず最新の日付を取得
            self.cursor.execute('SELECT MAX(submission_date) as latest_date FROM processed_reports')
            result = self.cursor.fetchone()
            latest_date = result['latest_date']
            
            if not latest_date:
                logger.warning("データベースに報告書がありません")
                return []
            
            # 最新の日付にマッチする報告書を全て取得
            self.cursor.execute('SELECT * FROM processed_reports WHERE submission_date = ? ORDER BY processed_at DESC', (latest_date,))
            rows = self.cursor.fetchall()
            latest_reports = [dict(row) for row in rows]
            
            logger.info(f"最新日付 {latest_date} の報告書が {len(latest_reports)} 件見つかりました")
            return latest_reports
        except sqlite3.Error as e:
            logger.error(f"最新日付報告書取得中にエラー: {e}")
            return []


# MySQL版のReportDatabaseクラス
class MySQLReportDatabase:
    def __init__(self, config=None):
        """
        MySQL データベース接続の初期化
        Args:
            config: MySQLの接続設定辞書。未指定の場合は環境変数から取得
        """
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python パッケージがインストールされていません")
        
        # デフォルト設定（環境変数から取得）
        self.config = config or {
            'host': os.environ.get('MYSQLHOST', 'interchange.proxy.rlwy.net'),
            'port': int(os.environ.get('MYSQLPORT', 3306)),
            'user': os.environ.get('MYSQLUSER', 'root'),
            'password': os.environ.get('MYSQLPASSWORD', 'TxoQDfJztZKuSREAmupOuZTCijlZFxFQ'),
            'database': os.environ.get('MYSQLDATABASE', 'railway'),
            'connection_timeout': 30,
            'buffered': True
        }
        
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """データベースへの接続を確立"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            logger.info(f"MySQLデータベースに接続しました: {self.config['host']}:{self.config['port']}/{self.config['database']}")
        except mysql.connector.Error as e:
            logger.error(f"MySQLデータベース接続エラー: {e}")
            raise
    
    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            logger.info("MySQLデータベース接続を閉じました")
    
    def create_tables(self):
        """必要なテーブルを作成"""
        try:
            cursor = self.conn.cursor()
            
            # 報告書テーブルの作成
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_reports (
                report_id VARCHAR(255) PRIMARY KEY,
                processed_at VARCHAR(255),
                target_company VARCHAR(255),
                security_code VARCHAR(50),
                report_type VARCHAR(255),
                holder_name VARCHAR(255),
                report_date VARCHAR(255),
                submission_date VARCHAR(255),
                INDEX idx_security_code (security_code),
                INDEX idx_holder_name (holder_name),
                INDEX idx_report_type (report_type)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            ''')
            
            self.conn.commit()
            cursor.close()
            logger.info("MySQLテーブルの作成が完了しました")
        except mysql.connector.Error as e:
            logger.error(f"MySQLテーブル作成エラー: {e}")
            self.conn.rollback()
            raise
    
    def is_already_processed(self, report_id):
        """
        報告書が既に処理済みかどうかを判定
        Args:
            report_id: 報告書ID
        Returns:
            bool: 処理済みかどうか
        """
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute('SELECT 1 FROM processed_reports WHERE report_id = %s', (report_id,))
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except mysql.connector.Error as e:
            logger.error(f"MySQL報告書チェック中にエラー: {e}")
            return False
    
    def mark_as_processed(self, report_info):
        """
        報告書を処理済みとしてマーク
        Args:
            report_info: 報告書情報の辞書
        Returns:
            bool: 処理が成功したかどうか
        """
        try:
            report_id = report_info.get('report_id')
            if not report_id:
                # report_idがない場合は、生成ロジックに従って作成
                security_code = report_info.get('security_code', '')
                submission_date = report_info.get('submission_date', '')
                report_date = report_info.get('report_date', '')
                report_type = report_info.get('report_type', '')
                holder_name = report_info.get('holder_name', '')
                
                # 日本語の日付から数字のみを抽出
                submission_numbers = ''.join(filter(str.isdigit, submission_date))
                report_numbers = ''.join(filter(str.isdigit, report_date))
                
                report_id = f"{security_code}_{submission_numbers}_{report_numbers}_{report_type}_{holder_name}"
            
            # 処理日時
            processed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT INTO processed_reports 
            (report_id, processed_at, target_company, security_code, 
            report_type, holder_name, report_date, submission_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            processed_at = VALUES(processed_at),
            target_company = VALUES(target_company),
            security_code = VALUES(security_code),
            report_type = VALUES(report_type),
            holder_name = VALUES(holder_name),
            report_date = VALUES(report_date),
            submission_date = VALUES(submission_date)
            ''', (
                report_id,
                processed_at,
                report_info.get('target_company', '不明'),
                report_info.get('security_code', '不明'),
                report_info.get('report_type', '不明'),
                report_info.get('holder_name', '不明'),
                report_info.get('report_date', '不明'),
                report_info.get('submission_date', '不明')
            ))
            
            self.conn.commit()
            cursor.close()
            logger.info(f"MySQL報告書 {report_id} を処理済みとして記録しました")
            return True
        
        except mysql.connector.Error as e:
            logger.error(f"MySQL報告書マーク中にエラー: {e}")
            self.conn.rollback()
            return False
    
    def get_all_processed_reports(self):
        """
        すべての処理済み報告書を取得
        Returns:
            list: 処理済み報告書のリスト
        """
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute('SELECT * FROM processed_reports ORDER BY processed_at DESC')
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except mysql.connector.Error as e:
            logger.error(f"MySQL処理済み報告書取得中にエラー: {e}")
            return []
    
    def search_reports(self, 
                       security_code=None, 
                       holder_name=None, 
                       report_type=None,
                       target_company=None,
                       limit=100):
        """
        条件に一致する報告書を検索
        Args:
            security_code: 証券コード
            holder_name: 保有者名
            report_type: 報告書種類
            target_company: 対象企業名
            limit: 取得する最大件数
        Returns:
            list: 一致する報告書のリスト
        """
        try:
            query = 'SELECT * FROM processed_reports WHERE 1=1'
            params = []
            
            if security_code:
                query += ' AND security_code = %s'
                params.append(security_code)
            
            if holder_name:
                query += ' AND holder_name LIKE %s'
                params.append(f'%{holder_name}%')
            
            if report_type:
                query += ' AND report_type = %s'
                params.append(report_type)
            
            if target_company:
                query += ' AND target_company LIKE %s'
                params.append(f'%{target_company}%')
            
            query += ' ORDER BY processed_at DESC LIMIT %s'
            params.append(limit)
            
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(query, params)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        
        except mysql.connector.Error as e:
            logger.error(f"MySQL報告書検索中にエラー: {e}")
            return []
    
    def get_report_counts_by_type(self):
        """
        報告書種類ごとの件数を取得
        Returns:
            dict: 報告書種類と件数の辞書
        """
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute('SELECT report_type, COUNT(*) as count FROM processed_reports GROUP BY report_type')
            rows = cursor.fetchall()
            cursor.close()
            return {row['report_type']: row['count'] for row in rows}
        except mysql.connector.Error as e:
            logger.error(f"MySQL集計中にエラー: {e}")
            return {}
    
    def get_latest_date_reports(self):
        """
        最新の日付に提出された報告書のみを取得
        Returns:
            list: 最新日付の報告書のリスト
        """
        try:
            cursor = self.conn.cursor(dictionary=True)
            
            # まず最新の日付を取得
            cursor.execute('SELECT MAX(submission_date) as latest_date FROM processed_reports')
            result = cursor.fetchone()
            latest_date = result['latest_date']
            
            if not latest_date:
                logger.warning("データベースに報告書がありません")
                cursor.close()
                return []
            
            # 最新の日付にマッチする報告書を全て取得
            cursor.execute('SELECT * FROM processed_reports WHERE submission_date = %s ORDER BY processed_at DESC', (latest_date,))
            latest_reports = cursor.fetchall()
            cursor.close()
            
            logger.info(f"最新日付 {latest_date} の報告書が {len(latest_reports)} 件見つかりました")
            return latest_reports
        except mysql.connector.Error as e:
            logger.error(f"MySQL最新日付報告書取得中にエラー: {e}")
            return []


# 環境変数に基づいてデータベース選択
def get_database():
    """
    環境に応じた適切なデータベースインスタンスを返す
    Returns:
        ReportDatabase または MySQLReportDatabase: データベースインスタンス
    """
    use_mysql = os.environ.get('USE_MYSQL', 'false').lower() == 'true'
    
    if use_mysql and MYSQL_AVAILABLE:
        try:
            return MySQLReportDatabase()
        except Exception as e:
            logger.error(f"MySQLデータベース初期化エラー: {e}、SQLiteにフォールバックします")
    
    return ReportDatabase()


# 使用例
if __name__ == "__main__":
    # データベース初期化
    db = ReportDatabase()
    
    # JSONファイルからデータをインポート
    import_count = db.import_from_json()
    print(f"{import_count}件のレコードをインポートしました")
    
    # 報告書種類ごとの集計
    report_counts = db.get_report_counts_by_type()
    for report_type, count in report_counts.items():
        print(f"{report_type}: {count}件")
    
    # 最新日付の報告書を取得（デバッグ用）
    latest_reports = db.get_latest_date_reports()
    print(f"\n最新日付の報告書: {len(latest_reports)}件")
    if latest_reports:
        print(f"最新日付: {latest_reports[0]['submission_date']}")
        # サンプルとして最初の数件を表示
        for i, report in enumerate(latest_reports[:3]):
            print(f"{i+1}. {report['target_company']} ({report['security_code']}) - {report['report_type']}")
        if len(latest_reports) > 3:
            print(f"他 {len(latest_reports) - 3} 件...")
    
    # 接続を閉じる
    db.close() 