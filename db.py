import sqlite3
import json
import os
import logging
from datetime import datetime
from pathlib import Path

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
    
    # 接続を閉じる
    db.close() 