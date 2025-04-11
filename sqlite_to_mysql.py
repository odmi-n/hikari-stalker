import sqlite3
import mysql.connector
import os
import logging
import argparse
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('sqlite_to_mysql')

# MySQLの接続情報（Railway環境用）- 直接値を指定
RAILWAY_MYSQL_CONFIG = {
    'host': 'mysql.railway.internal',  # Railway内部で使用するホスト名
    'port': 3306,
    'user': 'root',
    'password': 'TxoQDfJztZKuSREAmupOuZTCijlZFxFQ',
    'database': 'railway'
}

logger.info(f"MySQL接続情報: {RAILWAY_MYSQL_CONFIG['host']}:{RAILWAY_MYSQL_CONFIG['port']} ({RAILWAY_MYSQL_CONFIG['database']})")

# MySQLの接続情報（ローカル開発環境用）
LOCAL_MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',  # ローカルMySQLのパスワードを設定
    'database': 'edinet_db'  # ローカルで使用するデータベース名
}

# SQLiteのDBパス
SQLITE_DB_PATH = 'edinet_reports.db'

def get_sqlite_connection():
    """SQLiteデータベースへの接続を確立"""
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row  # 行を辞書形式で取得
        logger.info(f"SQLiteデータベースに接続しました: {SQLITE_DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"SQLiteデータベース接続エラー: {e}")
        raise

def get_mysql_connection(use_local=False):
    """MySQLデータベースへの接続を確立"""
    try:
        config = LOCAL_MYSQL_CONFIG if use_local else RAILWAY_MYSQL_CONFIG
        logger.info(f"接続設定: {config['host']}:{config['port']}/{config['database']}")
        conn = mysql.connector.connect(**config)
        logger.info(f"MySQLデータベースに接続しました: {config['host']}:{config['port']}/{config['database']}")
        return conn
    except mysql.connector.Error as e:
        logger.error(f"MySQLデータベース接続エラー: {e}")
        raise

def create_mysql_tables(mysql_conn):
    """MySQLに必要なテーブルを作成"""
    cursor = mysql_conn.cursor()
    
    try:
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
        
        mysql_conn.commit()
        logger.info("MySQLテーブルの作成が完了しました")
    except mysql.connector.Error as e:
        logger.error(f"MySQLテーブル作成エラー: {e}")
        mysql_conn.rollback()
        raise
    finally:
        cursor.close()

def migrate_data(use_local=False):
    """SQLiteからMySQLにデータを移行"""
    sqlite_conn = None
    mysql_conn = None
    
    try:
        # 接続を確立
        sqlite_conn = get_sqlite_connection()
        mysql_conn = get_mysql_connection(use_local)
        
        # MySQLにテーブルを作成
        create_mysql_tables(mysql_conn)
        
        # SQLiteからデータを取得
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute('SELECT * FROM processed_reports')
        rows = sqlite_cursor.fetchall()
        
        if not rows:
            logger.warning("SQLiteデータベースにデータがありません")
            return 0
        
        # MySQLに挿入
        mysql_cursor = mysql_conn.cursor()
        
        # まず既存のデータを削除（オプション）
        # mysql_cursor.execute('TRUNCATE TABLE processed_reports')
        
        # データを挿入
        insert_query = '''
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
        '''
        
        count = 0
        for row in rows:
            row_dict = dict(row)
            mysql_cursor.execute(insert_query, (
                row_dict['report_id'],
                row_dict['processed_at'],
                row_dict['target_company'],
                row_dict['security_code'],
                row_dict['report_type'],
                row_dict['holder_name'],
                row_dict['report_date'],
                row_dict['submission_date']
            ))
            count += 1
        
        mysql_conn.commit()
        logger.info(f"{count}件のレコードをMySQLにインポートしました")
        return count
    
    except Exception as e:
        logger.error(f"データ移行中にエラー: {e}")
        if mysql_conn:
            mysql_conn.rollback()
        return 0
    
    finally:
        # 接続を閉じる
        if sqlite_conn:
            sqlite_conn.close()
            logger.info("SQLite接続を閉じました")
        
        if mysql_conn:
            mysql_conn.close()
            logger.info("MySQL接続を閉じました")

def verify_migration(use_local=False):
    """移行が成功したか検証する"""
    sqlite_conn = None
    mysql_conn = None
    
    try:
        # 接続を確立
        sqlite_conn = get_sqlite_connection()
        mysql_conn = get_mysql_connection(use_local)
        
        # SQLiteのデータ件数を取得
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute('SELECT COUNT(*) as count FROM processed_reports')
        sqlite_count = sqlite_cursor.fetchone()['count']
        
        # MySQLのデータ件数を取得
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute('SELECT COUNT(*) as count FROM processed_reports')
        mysql_count = mysql_cursor.fetchone()['count']
        
        logger.info(f"SQLiteレコード数: {sqlite_count}")
        logger.info(f"MySQLレコード数: {mysql_count}")
        
        if sqlite_count == mysql_count:
            logger.info("✅ データ移行は成功しました！レコード数が一致しています。")
            return True
        else:
            logger.warning("❌ データ移行の検証に失敗しました。レコード数が一致していません。")
            return False
    
    except Exception as e:
        logger.error(f"移行検証中にエラー: {e}")
        return False
    
    finally:
        # 接続を閉じる
        if sqlite_conn:
            sqlite_conn.close()
        
        if mysql_conn:
            mysql_conn.close()

if __name__ == "__main__":
    try:
        # コマンドライン引数の解析
        parser = argparse.ArgumentParser(description='SQLiteからMySQLへデータを移行するツール')
        parser.add_argument('--local', action='store_true', help='ローカルのMySQLサーバーを使用する')
        args = parser.parse_args()
        
        use_local = args.local
        env_type = "ローカル" if use_local else "Railway"
        
        logger.info(f"SQLiteから{env_type} MySQLへのデータ移行を開始します...")
        
        # データを移行
        migrated_count = migrate_data(use_local)
        
        if migrated_count > 0:
            # 移行を検証
            verify_migration(use_local)
        
        logger.info("データ移行プロセスが完了しました")
    
    except Exception as e:
        logger.error(f"移行プロセス中にエラーが発生しました: {e}") 