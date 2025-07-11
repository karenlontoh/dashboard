from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from fastapi.responses import FileResponse
import os, psycopg2
from datetime import datetime
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


app = FastAPI()
load_dotenv()
months_line = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Bisa diganti ke asal frontend kamu, misal ["http://localhost:8080"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


# Mount folder frontend
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")

# Serve index.html saat root (/) diakses
@app.get("/")
def serve_index():
    return FileResponse(os.path.join("frontend","html","index.html"))

@app.get("/dashboard")
def get_dashboard_data(year: int = None):
    now = datetime.now()
    year = year or now.year
    current_month = now.month

    # Hitung bulan sebelumnya (perhatikan jika bulan sekarang Januari)
    if current_month == 1:
        last_month = 12
        last_month_year = year - 1
    else:
        last_month = current_month - 1
        last_month_year = year

    conn = get_connection()
    cursor = conn.cursor()

    # Total disbursement untuk tahun berjalan
    cursor.execute("""
        SELECT COALESCE(SUM(issue_amount), 0)
        FROM issue_record
        WHERE disbursement_method != 'FAKE'
          AND type = 'DEFAULT'
          AND status = 'SUCCEED'
          AND EXTRACT(YEAR FROM create_time) = %s
    """, (year,))
    ap_disbursement_ytd = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_disbursement
        WHERE pay_status = 'SUCCESS'
          AND EXTRACT(YEAR FROM create_time) = %s
    """, (year,))
    cn_disbursement_ytd = cursor.fetchone()[0]

    disbursement_ytd = ap_disbursement_ytd + cn_disbursement_ytd

    # MONTHLY
    cursor.execute("""
        SELECT COALESCE(SUM(issue_amount), 0)
        FROM issue_record
        WHERE disbursement_method != 'FAKE'
          AND type = 'DEFAULT'
          AND status = 'SUCCEED'
          AND EXTRACT(YEAR FROM create_time) = %s
          AND EXTRACT(MONTH FROM create_time) = %s
    """, (year, current_month))
    ap_disbursement_mtd = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_disbursement
        WHERE pay_status = 'SUCCESS'
          AND EXTRACT(YEAR FROM create_time) = %s
          AND EXTRACT(MONTH FROM create_time) = %s
    """, (year, current_month))
    cn_disbursement_mtd = cursor.fetchone()[0]

    disbursement_mtd = ap_disbursement_mtd + cn_disbursement_mtd
    # Total dari repayment_record
    cursor.execute("""
        SELECT COALESCE(SUM(arrived_amount), 0)
        FROM repayment_record
        WHERE EXTRACT(YEAR FROM transaction_time) = %s
    """, (year,))
    ap_repayment = cursor.fetchone()[0]
    
    # Total dari cn_repayment
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_repayment
        WHERE EXTRACT(YEAR FROM transaction_date) = %s
    """, (year,))
    cn_repayment = cursor.fetchone()[0]
    
    # Jumlah total repayment
    repayment = ap_repayment + cn_repayment

    # Outstanding khusus bulan sebelumnya
    cursor.execute("""
        SELECT COALESCE(SUM(total_outstanding), 0)
        FROM outstanding
        WHERE 
          keterangan ILIKE ANY (ARRAY['MALE', 'FEMALE'])
          AND EXTRACT(MONTH FROM month) = %s
          AND EXTRACT(YEAR FROM month) = %s
          AND outstanding_type = 'AP'
    """, (last_month, last_month_year))
    ap_outstanding = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COALESCE(SUM(total_outstanding), 0)
        FROM outstanding
        WHERE 
          keterangan ILIKE ANY (ARRAY['MALE', 'FEMALE'])
          AND EXTRACT(MONTH FROM month) = %s
          AND EXTRACT(YEAR FROM month) = %s
          AND outstanding_type = 'CN'
    """, (last_month, last_month_year))
    cn_outstanding = cursor.fetchone()[0]

    # Jumlah total outstanding
    outstanding = ap_outstanding + cn_outstanding
    
    # Menutup koneksi
    cursor.close()
    conn.close()

    return {
        "disbursement_ytd": disbursement_ytd,
        "ap_ytd": ap_disbursement_ytd,
        "cn_ytd": cn_disbursement_ytd,
        "disbursement_mtd": disbursement_mtd,
        "ap_mtd": ap_disbursement_mtd,
        "cn_mtd": cn_disbursement_mtd,
        "repayment": repayment,
        "outstanding": outstanding
    }


@app.get("/disbursement-line")
def get_disbursement_line_data(year: int = None):
    now = datetime.now()
    year = year or now.year

    conn = get_connection()
    cursor = conn.cursor()

    result = []

    for month in range(1, 13):
        # Adapundi
        cursor.execute("""
            SELECT COALESCE(SUM(issue_amount), 0)
            FROM issue_record
            WHERE disbursement_method != 'FAKE'
              AND type = 'DEFAULT'
              AND status = 'SUCCEED'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
        """, (year, month))
        adapundi = cursor.fetchone()[0]

        # Credinex
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM cn_disbursement
            WHERE pay_status = 'SUCCESS'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
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

@app.get("/repayment-line")
def get_repayment_line_data(year: int = None):
    now = datetime.now()
    year = year or now.year

    conn = get_connection()
    cursor = conn.cursor()
    result = []

    for month in range(1, 13):
        cursor.execute("""
            SELECT COALESCE(SUM(arrived_amount), 0)
            FROM repayment_record
            WHERE EXTRACT(YEAR FROM transaction_time) = %s
              AND EXTRACT(MONTH FROM transaction_time) = %s
        """, (year, month))
        adapundi = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM cn_repayment
            WHERE EXTRACT(YEAR FROM transaction_date) = %s
              AND EXTRACT(MONTH FROM transaction_date) = %s
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

@app.get("/outstanding-line")
def get_outstanding_line_data(year: int = None):
    now = datetime.now()
    year = year or now.year

    conn = get_connection()
    cursor = conn.cursor()
    result = []

    for month in range(1, 13):
        cursor.execute("""
            SELECT COALESCE(SUM(total_outstanding), 0)
            FROM outstanding
            WHERE keterangan ILIKE ANY (ARRAY['MALE', 'FEMALE'])
              AND EXTRACT(YEAR FROM month) = %s
              AND EXTRACT(MONTH FROM month) = %s
              AND outstanding_type = 'AP'
        """, (year, month))
        adapundi = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(total_outstanding), 0)
            FROM outstanding  
            WHERE keterangan ILIKE ANY (ARRAY['MALE', 'FEMALE'])
              AND EXTRACT(YEAR FROM month) = %s
              AND EXTRACT(MONTH FROM month) = %s
              AND outstanding_type = 'CN'
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

@app.get("/disbursement-by-lender")
def disbursement_by_lender(year: int = None):
    year = year or datetime.now().year
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            CASE 
                WHEN lender = 'SEABANK_V2' THEN 'SEABANK'
                WHEN lender = 'STAR_DANA' THEN 'STARDANA'
            ELSE lender
        END AS lender_alias, COALESCE(SUM(issue_amount), 0)
        FROM issue_record
        WHERE disbursement_method != 'FAKE'
          AND type = 'DEFAULT'
          AND status = 'SUCCEED'
          AND EXTRACT(YEAR FROM create_time) = %s
        GROUP BY lender_alias
        ORDER BY SUM(issue_amount) DESC
    """, (year,))
    result = [{"label": row[0], "value": float(row[1])} for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    return result

@app.get("/repayment-by-channel")
def repayment_by_channel(year: int = None):
    year = year or datetime.now().year
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            CASE
                WHEN deposit_channel = 'INSTAMONEY_V2' THEN 'INSTAMONEY'
                WHEN deposit_channel = 'VIRTUAL_CHNNEL' THEN 'UPFRONT FEE' 
                WHEN deposit_channel = 'FASPAY_V2' THEN 'FASPAY' 
                WHEN deposit_channel = 'FASPAY_EWALLET' THEN 'FASPAY E-WALLET'
                WHEN deposit_channel = 'AYO_DD' THEN 'AYO' 
                WHEN deposit_channel = 'XENDIT' THEN 'WAIVE'
            ELSE deposit_channel
        END AS channel_alias, COALESCE(SUM(arrived_amount), 0)
        FROM repayment_record
        WHERE EXTRACT(YEAR FROM transaction_time) = %s
        GROUP BY channel_alias
        ORDER BY SUM(arrived_amount) DESC
    """, (year,))
    result = [{"label": row[0], "value": float(row[1])} for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return result

@app.get("/outstanding-by-lender")
def outstanding_by_lender(year: int = None):
    now = datetime.now()
    year = year or now.year

    # Hitung bulan sebelumnya
    if now.month == 1:
        last_month = 12
        last_month_year = year - 1
    else:
        last_month = now.month - 1
        last_month_year = year

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT keterangan, SUM(total_outstanding) as total
        FROM outstanding
        WHERE group_name = 'Lender'
          AND EXTRACT(YEAR FROM month) = %s
          AND EXTRACT(MONTH FROM month) = %s
        GROUP BY keterangan
        ORDER BY total DESC
    """, (last_month_year, last_month))

    result = [{"label": row[0], "value": float(row[1])} for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return result

@app.get("/disbursement-daily")
def get_disbursement_daily(year: int, month: int, tipe: str = "ALL"):
    conn = get_connection()
    cursor = conn.cursor()

    data_ap = {}
    data_cn = {}

    if tipe in ["ALL", "AP"]:
        cursor.execute("""
            SELECT DATE(create_time), SUM(issue_amount)
            FROM issue_record
            WHERE disbursement_method != 'FAKE'
              AND type = 'DEFAULT'
              AND status = 'SUCCEED'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
            GROUP BY DATE(create_time)
        """, (year, month))
        for row in cursor.fetchall():
            date_str = row[0].strftime("%Y-%m-%d")
            data_ap[date_str] = float(row[1])

    if tipe in ["ALL", "CN"]:
        cursor.execute("""
            SELECT DATE(create_time), SUM(amount)
            FROM cn_disbursement
            WHERE pay_status = 'SUCCESS'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
            GROUP BY DATE(create_time)
        """, (year, month))
        for row in cursor.fetchall():
            date_str = row[0].strftime("%Y-%m-%d")
            data_cn[date_str] = float(row[1])

    result = []

    if tipe == "ALL":
        all_dates = set(data_ap.keys()).union(data_cn.keys())
        for date in sorted(all_dates):
            ap_amount = data_ap.get(date, 0)
            cn_amount = data_cn.get(date, 0)
            result.append({
                "date": date,
                "amount": ap_amount + cn_amount,
                "source": "Total"
            })
    elif tipe == "AP":
        for date in sorted(data_ap.keys()):
            result.append({
                "date": date,
                "amount": data_ap[date],
                "source": "Adapundi"
            })
    elif tipe == "CN":
        for date in sorted(data_cn.keys()):
            result.append({
                "date": date,
                "amount": data_cn[date],
                "source": "Credinex"
            })

    cursor.close()
    conn.close()
    return result


@app.get("/disbursement-breakdown")
def get_disbursement_breakdown(year: int, month: int, tipe: str = "ALL"):
    conn = get_connection()
    cursor = conn.cursor()

    lender_data = {}
    method_data = {}

    def add_to_dict(target_dict, key, amount):
        if key in target_dict:
            target_dict[key] += float(amount)
        else:
            target_dict[key] = float(amount)

    if tipe in ["ALL", "AP"]:
        cursor.execute("""
            SELECT lender, SUM(issue_amount)
            FROM issue_record
            WHERE disbursement_method != 'FAKE'
              AND type = 'DEFAULT'
              AND status = 'SUCCEED'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
            GROUP BY lender
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(lender_data, row[0], row[1])

        cursor.execute("""
            SELECT disbursement_method, SUM(issue_amount)
            FROM issue_record
            WHERE disbursement_method != 'FAKE'
              AND type = 'DEFAULT'
              AND status = 'SUCCEED'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
            GROUP BY disbursement_method
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(method_data, row[0], row[1])

    if tipe in ["ALL", "CN"]:
        cursor.execute("""
            SELECT capital_lender, SUM(amount)
            FROM cn_disbursement
            WHERE pay_status = 'SUCCESS'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
            GROUP BY capital_lender
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(lender_data, row[0], row[1])

        cursor.execute("""
            SELECT channel_type, SUM(amount)
            FROM cn_disbursement
            WHERE pay_status = 'SUCCESS'
              AND EXTRACT(YEAR FROM create_time) = %s
              AND EXTRACT(MONTH FROM create_time) = %s
            GROUP BY channel_type
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(method_data, row[0], row[1])

    cursor.close()
    conn.close()

    result = {
        "lender": [{"label": k, "value": v} for k, v in sorted(lender_data.items(), key=lambda x: x[1], reverse=True)],
        "method": [{"label": k, "value": v} for k, v in sorted(method_data.items(), key=lambda x: x[1], reverse=True)]
    }

    return result


@app.get("/dashboard/mtd")
def get_dashboard_mtd(year: int = None, month: int = None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    conn = get_connection()
    cursor = conn.cursor()

    # AP MTD
    cursor.execute("""
        SELECT COALESCE(SUM(issue_amount), 0)
        FROM issue_record
        WHERE disbursement_method != 'FAKE'
          AND type = 'DEFAULT'
          AND status = 'SUCCEED'
          AND EXTRACT(YEAR FROM create_time) = %s
          AND EXTRACT(MONTH FROM create_time) = %s
    """, (year, month))
    ap_disbursement_mtd = cursor.fetchone()[0]

    # CN MTD
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_disbursement
        WHERE pay_status = 'SUCCESS'
          AND EXTRACT(YEAR FROM create_time) = %s
          AND EXTRACT(MONTH FROM create_time) = %s
    """, (year, month))
    cn_disbursement_mtd = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return {
        "disbursement_mtd": ap_disbursement_mtd + cn_disbursement_mtd,
        "ap_mtd": ap_disbursement_mtd,
        "cn_mtd": cn_disbursement_mtd,
    }

@app.get("/dashboard/ytd")
def get_dashboard_mtd(year: int = None, month: int = None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    conn = get_connection()
    cursor = conn.cursor()

    # AP MTD
    cursor.execute("""
        SELECT COALESCE(SUM(issue_amount), 0)
        FROM issue_record
        WHERE disbursement_method != 'FAKE'
          AND type = 'DEFAULT'
          AND status = 'SUCCEED'
          AND EXTRACT(YEAR FROM create_time) = %s
          AND EXTRACT(MONTH FROM create_time) <= %s
    """, (year, month))
    ap_disbursement_ytd = cursor.fetchone()[0]

    # CN MTD
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_disbursement
        WHERE pay_status = 'SUCCESS'
          AND EXTRACT(YEAR FROM create_time) = %s
          AND EXTRACT(MONTH FROM create_time) <= %s
    """, (year, month))
    cn_disbursement_ytd = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return {
        "disbursement_ytd": ap_disbursement_ytd + cn_disbursement_ytd,
        "ap_ytd": ap_disbursement_ytd,
        "cn_ytd": cn_disbursement_ytd,
    }

@app.get("/repayment/mtd")
def get_repayment_mtd(year: int = None, month: int = None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    conn = get_connection()
    cursor = conn.cursor()

    # AP Repayment MTD
    cursor.execute("""
        SELECT COALESCE(SUM(arrived_amount), 0)
        FROM repayment_record
        WHERE EXTRACT(YEAR FROM transaction_time) = %s
          AND EXTRACT(MONTH FROM transaction_time) = %s
    """, (year, month))
    ap_mtd = cursor.fetchone()[0]

    # CN Repayment MTD
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_repayment
        WHERE EXTRACT(YEAR FROM transaction_date) = %s
          AND EXTRACT(MONTH FROM transaction_date) = %s
    """, (year, month))
    cn_mtd = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return {
        "repayment_mtd": ap_mtd + cn_mtd,
        "ap_mtd": ap_mtd,
        "cn_mtd": cn_mtd,
    }

@app.get("/repayment/ytd")
def get_repayment_ytd(year: int = None, month: int = None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    conn = get_connection()
    cursor = conn.cursor()

    # AP Repayment YTD
    cursor.execute("""
        SELECT COALESCE(SUM(arrived_amount), 0)
        FROM repayment_record
        WHERE EXTRACT(YEAR FROM transaction_time) = %s
          AND EXTRACT(MONTH FROM transaction_time) <= %s
    """, (year, month))
    ap_ytd = cursor.fetchone()[0]

    # CN Repayment YTD
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM cn_repayment
        WHERE EXTRACT(YEAR FROM transaction_date) = %s
          AND EXTRACT(MONTH FROM transaction_date) <= %s
    """, (year, month))
    cn_ytd = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return {
        "repayment_ytd": ap_ytd + cn_ytd,
        "ap_ytd": ap_ytd,
        "cn_ytd": cn_ytd,
    }

@app.get("/repayment-daily")
def get_repayment_daily(year: int, month: int, tipe: str = "ALL"):
    conn = get_connection()
    cursor = conn.cursor()

    data_ap = {}
    data_cn = {}

    if tipe in ["ALL", "AP"]:
        cursor.execute("""
            SELECT DATE(transaction_time), SUM(arrived_amount)
            FROM repayment_record
            WHERE EXTRACT(YEAR FROM transaction_time) = %s
              AND EXTRACT(MONTH FROM transaction_time) = %s
            GROUP BY DATE(transaction_time)
        """, (year, month))
        for row in cursor.fetchall():
            date_str = row[0].strftime("%Y-%m-%d")
            data_ap[date_str] = float(row[1])

    if tipe in ["ALL", "CN"]:
        cursor.execute("""
            SELECT DATE(transaction_date), SUM(amount)
            FROM cn_repayment
            WHERE EXTRACT(YEAR FROM transaction_date) = %s
              AND EXTRACT(MONTH FROM transaction_date) = %s
            GROUP BY DATE(transaction_date)
        """, (year, month))
        for row in cursor.fetchall():
            date_str = row[0].strftime("%Y-%m-%d")
            data_cn[date_str] = float(row[1])

    result = []

    if tipe == "ALL":
        all_dates = set(data_ap.keys()).union(data_cn.keys())
        for date in sorted(all_dates):
            ap_amount = data_ap.get(date, 0)
            cn_amount = data_cn.get(date, 0)
            result.append({
                "date": date,
                "amount": ap_amount + cn_amount,
                "source": "Total"
            })
    elif tipe == "AP":
        for date in sorted(data_ap.keys()):
            result.append({
                "date": date,
                "amount": data_ap[date],
                "source": "Adapundi"
            })
    elif tipe == "CN":
        for date in sorted(data_cn.keys()):
            result.append({
                "date": date,
                "amount": data_cn[date],
                "source": "Credinex"
            })

    cursor.close()
    conn.close()
    return result

@app.get("/repayment-breakdown")
def get_repayment_breakdown(year: int, month: int, tipe: str = "ALL"):
    conn = get_connection()
    cursor = conn.cursor()

    channel_data = {}
    method_data = {}

    def add_to_dict(target_dict, key, amount):
        if key in target_dict:
            target_dict[key] += float(amount)
        else:
            target_dict[key] = float(amount)

    if tipe in ["ALL", "AP"]:
        # By deposit_channel
        cursor.execute("""
            SELECT 
                CASE
                WHEN deposit_channel = 'INSTAMONEY_V2' THEN 'INSTAMONEY'
                WHEN deposit_channel = 'VIRTUAL_CHNNEL' THEN 'UPFRONT FEE' 
                WHEN deposit_channel = 'FASPAY_V2' THEN 'FASPAY' 
                WHEN deposit_channel = 'FASPAY_EWALLET' THEN 'FASPAY E-WALLET'
                WHEN deposit_channel = 'AYO_DD' THEN 'AYO' 
                WHEN deposit_channel = 'XENDIT' THEN 'WAIVE'
                ELSE deposit_channel
            END AS deposit_channel_alias, SUM(arrived_amount)
            FROM repayment_record
            WHERE EXTRACT(YEAR FROM transaction_time) = %s
              AND EXTRACT(MONTH FROM transaction_time) = %s
            GROUP BY deposit_channel_alias
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(channel_data, row[0] or "UNKNOWN", row[1])

        # By deposit_method (hanya AP)
        cursor.execute("""
            SELECT deposit_method, SUM(arrived_amount)
            FROM repayment_record
            WHERE EXTRACT(YEAR FROM transaction_time) = %s
              AND EXTRACT(MONTH FROM transaction_time) = %s
            GROUP BY deposit_method
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(method_data, row[0] or "UNKNOWN", row[1])

    if tipe in ["ALL", "CN"]:
        # By deposit_channel (alias channel_type di CN)
        cursor.execute("""
            SELECT channel_type, SUM(amount)
            FROM cn_repayment
            WHERE EXTRACT(YEAR FROM transaction_date) = %s
              AND EXTRACT(MONTH FROM transaction_date) = %s
            GROUP BY channel_type
        """, (year, month))
        for row in cursor.fetchall():
            add_to_dict(channel_data, row[0] or "UNKNOWN", row[1])

        # Tidak ada deposit_method di CN → skip bagian ini
        # Bisa tambahkan logika jika ingin menandai bahwa ini tidak tersedia

    cursor.close()
    conn.close()

    return {
        "channel": [{"label": k, "value": v} for k, v in sorted(channel_data.items(), key=lambda x: x[1], reverse=True)],
        "method": [{"label": k, "value": v} for k, v in sorted(method_data.items(), key=lambda x: x[1], reverse=True)]
    }

@app.get("/outstanding-dashboard")
def get_outstanding_dashboard(year: int, month: int, tipe: str = "ALL"):
    conn = get_connection()
    cursor = conn.cursor()

    def sum_fields(fields, filters=""):
        query = f"""
            SELECT {", ".join([f"SUM({f})" for f in fields])}
            FROM outstanding
            WHERE EXTRACT(YEAR FROM month) = %s
              AND EXTRACT(MONTH FROM month) = %s
              AND keterangan IN ('FEMALE', 'MALE')
              {filters}
        """
        cursor.execute(query, (year, month))
        return cursor.fetchone()

    # Filter by type
    tipe_filter = ""
    if tipe != "ALL":
        tipe_filter = f" AND outstanding_type = '{tipe}'"

    # Row 1: Outstanding
    out_fields = [
        "less_zero_outstanding", "up_zero_outstanding",
        "thirty_outstanding", "sixty_outstanding", "total_outstanding"
    ]
    out_vals = sum_fields(out_fields, tipe_filter)

    # Row 2: Borrowers
    bor_fields = [
        "less_zero_unique", "up_zero_unique",
        "thirty_unique", "sixty_unique", "total_unique"
    ]
    bor_vals = sum_fields(bor_fields, tipe_filter)

    # Row 3: Grouped Outstanding (Lender, Age)
    def group_outstanding(group_name):
        cursor.execute(f"""
            SELECT keterangan, SUM(total_outstanding)
            FROM outstanding
            WHERE EXTRACT(YEAR FROM month) = %s
              AND EXTRACT(MONTH FROM month) = %s
              AND group_name = %s
              {tipe_filter}
            GROUP BY keterangan
        """, (year, month, group_name))
        return [{"label": r[0], "value": float(r[1])} for r in cursor.fetchall()]

    # Row 4: Grouped Outstanding (Apply Purpose, Gender)
    result = {
        "row1": {
            "(<=0) Outstanding": out_vals[0],
            "(1,30) Outstanding": out_vals[1],
            "(31,60) Outstanding": out_vals[2],
            "(61,90) Outstanding": out_vals[3],
            "Total Outstanding": out_vals[4]
        },
        "row2": {
            "(<=0) Borrowers": bor_vals[0],
            "(1,30) Borrowers": bor_vals[1],
            "(31,60) Borrowers": bor_vals[2],
            "(61,90) Borrowers": bor_vals[3],
            "Total Borrowers": bor_vals[4]
        },
        "row3": {
            "Outstanding per Lender": group_outstanding("Lender"),
            "Outstanding by Age": group_outstanding("Age")
        },
        "row4": {
            "Outstanding per Apply Purpose": group_outstanding("Apply Purpose"),
            "Outstanding by Gender": group_outstanding("Gender")
        }
    }

    cursor.close()
    conn.close()
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
