import datetime
from hikariget import fetch_reports
from parser import parse_and_filter_reports
from notifier import send_line_message
from db import ReportDatabase

# 定数設定（必要に応じてconfig.pyに分離）
DOWNLOAD_DIR = "edinet_downloads"

def check_database():
    """データベースの状態を確認"""
    print("🔍 データベース接続をチェック中...")
    try:
        db = ReportDatabase()
        report_counts = db.get_report_counts_by_type()
        total = sum(report_counts.values())
        print(f"✅ データベース接続成功: 合計{total}件のレコード")
        
        # レポートタイプごとの件数を表示
        for report_type, count in report_counts.items():
            print(f"  - {report_type}: {count}件")
        
        # 最新の5件を表示
        reports = db.search_reports(limit=5)
        if reports:
            print("\n📋 最新の5件:")
            for report in reports:
                print(f"  - {report['target_company']} ({report['security_code']}) - {report['report_type']} - {report['processed_at']}")
        
        db.close()
        return True
    except Exception as e:
        print(f"❌ データベース接続エラー: {e}")
        return False

def main():
    print("🚀 [main] 自動通知処理を開始します")

    # データベース接続を確認
    check_database()

    # 本日の日付（形式：2025-04-11）
    target_date = datetime.date.today().strftime("%Y-%m-%d")

    # 1. EDINETからZipファイルを取得
    print(f"📥 [main] {target_date}の報告書を取得中...")
    fetch_reports(target_date)

    # 2. 解凍・パース・メッセージ整形（再通知除外もここで実施）
    print("🗂️ [main] ファイル解析中...")
    messages = parse_and_filter_reports(DOWNLOAD_DIR)

    # 3. 通知処理
    print("📡 [main] LINE通知を開始...")
    for message in messages:
        send_line_message(message)

    print("✅ [main] 全ての処理が完了しました。")

if __name__ == "__main__":
    main()
