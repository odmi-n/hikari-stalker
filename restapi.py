# restapi.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import sqlite3
import os

app = FastAPI()

DB_PATH = os.environ.get("DB_PATH", "edinet_reports.db")

@app.get("/")
def root():
    return {"message": "EDINET REST API is running"}

@app.get("/api/latest_companies")
def get_latest_companies():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 最新の日付を取得
        cursor.execute("SELECT MAX(date) FROM reports")
        latest_date = cursor.fetchone()[0]

        if not latest_date:
            return JSONResponse(content={"error": "No data found"}, status_code=404)

        # その日付に該当する全企業を取得
        cursor.execute("""
            SELECT company_name, stock_code, document_type
            FROM reports
            WHERE date = ?
        """, (latest_date,))
        rows = cursor.fetchall()
        conn.close()

        # JSON形式で返却
        return {
            "latest_date": latest_date,
            "companies": [
                {"name": row[0], "code": row[1], "type": row[2]}
                for row in rows
            ]
        }
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
