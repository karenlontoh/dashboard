from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from fastapi.responses import FileResponse
import os, psycopg2
from datetime import datetime
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import psycopg2

# Load env dan koneksi sekali saat startup
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
try:
    conn = psycopg2.connect(DATABASE_URL)
    print("✅ DB connected")
except Exception as e:
    print("❌ DB connection failed:", e)
    conn = None

# Fungsi reusable untuk koneksi per request
def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

# def get_connection():
#     return psycopg2.connect(
#         host=os.getenv("DB_HOST"),
#         port=os.getenv("DB_PORT"),
#         dbname=os.getenv("DB_NAME"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD")
#     )
app = FastAPI()
load_dotenv()
months_line = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount folder frontend
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")
app.mount("/css", StaticFiles(directory="frontend/dist/css"), name="css")
app.mount("/js", StaticFiles(directory="frontend/dist/js"), name="js")
app.mount("/assets", StaticFiles(directory="frontend/assets"), name="assets")

@app.get("/{page_name}")
def serve_page(page_name: str):
    # Kalau sudah ada .html, jangan tambahkan lagi
    if not page_name.endswith(".html"):
        page_name += ".html"

    file_path = os.path.join("frontend", "html", page_name)
    print("🔍 Looking for:", file_path)
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "Page not found"}, 404

# Serve index.html
@app.get("/")
def serve_index():
    return FileResponse(os.path.join("frontend", "html", "index.html"))

@app.get("/data/dashboard")
def get_dashboard_data(year: int = None):
    try:
        now = datetime.now()
        year = year or now.year
        current_month = now.month

        # Hitung bulan sebelumnya
        if current_month == 1:
            last_month = 12
            last_month_year = year - 1
        else:
            last_month = current_month - 1
            last_month_year = year

        conn = get_connection()
        cursor = conn.cursor()

        # --- DISBURSEMENT YTD ---
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Disbursement'
              AND product = 'Adapundi'
              AND EXTRACT(YEAR FROM period) = %s
        """, (year,))
        ap_disbursement_ytd = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Disbursement'
              AND product = 'Credinex'
              AND EXTRACT(YEAR FROM period) = %s
        """, (year,))
        cn_disbursement_ytd = cursor.fetchone()[0]

        disbursement_ytd = ap_disbursement_ytd + cn_disbursement_ytd

        # --- REPAYMENT YTD ---
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Repayment'
              AND product = 'Adapundi'
              AND EXTRACT(YEAR FROM period) = %s
        """, (year,))
        ap_repayment = cursor.fetchone()[0]

        cursor.execute("""
           SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Repayment'
              AND product = 'Credinex'
              AND EXTRACT(YEAR FROM period) = %s
        """, (year,))
        cn_repayment = cursor.fetchone()[0]

        repayment = ap_repayment + cn_repayment

        # --- OUTSTANDING Last Month ---
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Outstanding'
              AND product = 'Adapundi'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (last_month_year, last_month))
        ap_outstanding = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Outstanding'
              AND product = 'Credinex'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (last_month_year, last_month))
        cn_outstanding = cursor.fetchone()[0]

        outstanding = ap_outstanding + cn_outstanding

        cursor.close()
        conn.close()

        return {
            "disbursement_ytd": disbursement_ytd,
            "ap_ytd": ap_disbursement_ytd,
            "cn_ytd": cn_disbursement_ytd,
            "repayment": repayment,
            "outstanding": outstanding
        }

    except Exception as e:
        print("ERROR:", e)
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/data/disbursement-line")
def get_disbursement_line_data(year: int = None):
    now = datetime.now()
    year = year or now.year

    conn = get_connection()
    cursor = conn.cursor()

    result = []

    for month in range(1, 13):
        # Adapundi
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Disbursement'
              AND product = 'Adapundi'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (year, month))
        adapundi = cursor.fetchone()[0]

        # Credinex
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Disbursement'
              AND product = 'Credinex'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (year, month))
        credinex = cursor.fetchone()[0]

        if adapundi == 0 and credinex == 0:
            break
        
        result.append({
            "y": months_line[month - 1],
            "adapundi": float(adapundi),
            "credinex": float(credinex)
        })

    cursor.close()
    conn.close()

    return result

@app.get("/data/repayment-line")
def get_repayment_line_data(year: int = None):
    now = datetime.now()
    year = year or now.year

    conn = get_connection()
    cursor = conn.cursor()
    result = []

    for month in range(1, 13):
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Repayment'
              AND product = 'Adapundi'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (year, month))
        adapundi = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Repayment'
              AND product = 'Credinex'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (year, month))
        credinex = cursor.fetchone()[0]

        if adapundi == 0 and credinex == 0:
            break

        result.append({
            "y": months_line[month - 1],
            "adapundi": float(adapundi),
            "credinex": float(credinex)
        })

    cursor.close()
    conn.close()
    return result

@app.get("/data/outstanding-line")
def get_outstanding_line_data(year: int = None):
    now = datetime.now()
    year = year or now.year

    conn = get_connection()
    cursor = conn.cursor()
    result = []

    for month in range(1, 13):
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data
            WHERE type = 'Outstanding'
              AND product = 'Adapundi'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (year, month))
        adapundi = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM dashboard_data  
            WHERE type = 'Outstanding'
              AND product = 'Credinex'
              AND EXTRACT(YEAR FROM period) = %s
              AND EXTRACT(MONTH FROM period) = %s
        """, (year, month))
        credinex = cursor.fetchone()[0]

        if adapundi == 0 and credinex == 0:
            break

        result.append({
            "y": months_line[month - 1],
            "adapundi": float(adapundi),
            "credinex": float(credinex)
        })

    cursor.close()
    conn.close()
    return result