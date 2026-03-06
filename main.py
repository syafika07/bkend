import io
import uvicorn
from fastapi import FastAPI, File, UploadFile, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine, text
import tempfile
import os
from datetime import datetime, timedelta
import pdfplumber

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = "postgresql://postgres.uonphwbbgemsvqzrdcwp:Ika_15050107@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
#DB_URL = "postgresql://postgres.qcbzsjpbexrfthbcgmhz:Awangika_15050107@aws-1-ap-northeast-1.pooler.supabase.com:5432/postgres" #syafika

engine = create_engine(
    DB_URL,
    pool_size=40,
    max_overflow=8,
    pool_timeout=30

)

HEADER_REMOVE_PATTERNS = [
    r"PLAZA\s*:", r"DATE\s*:", r"TIME\s*:", r"PAGE\s*:",
    r"PLAZA COMPUTER SYSTEM", r"INDIVIDUAL TRANSACTION",
    r"PRINT NUMBER", r"Operational Date", r"Lane No",
    r"BOJ\s*:", r"EOJ\s*:", r"Plaza No", r"Job No",
    r"Badge No", r"Name", r"^-+$", r"SDEOF", r"PROGRAM:"
]

def clean_pdf_lines(text: str):
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(re.search(h, line) for h in HEADER_REMOVE_PATTERNS):
            continue
        cleaned.append(line)
    return cleaned

def is_valid_money_format(val):
    if not val or val.strip() == "":
        return False
    return bool(re.fullmatch(r'\d+\.\d{2}', val.strip()))

def normalize_money(val):
    if not val or val.strip() == "":
        return "0.00"
    try:
        cleaned = re.sub(r"[^\d.-]", "", val)
        num = float(cleaned)
        return f"{num:.2f}"
    except (ValueError, TypeError):
        return "0.00"

def extract_plaza_no_from_text(text: str) -> str:
    match = re.search(r'Plaza\s*No\s*:\s*(\d{3})', text)
    if match:
        return match.group(1)
    return "000"

import re

def extract_lane_job_from_text(text: str):
    lane_match = re.search(r'Lane\s*No\s*:\s*([A-Za-z0-9]+)', text)
    job_match = re.search(r'Job\s*No\s*:\s*(\d+)', text)

    lane_no = lane_match.group(1) if lane_match else None
    job_no = int(job_match.group(1)) if job_match else None

    # Tentukan error spesifik
    if not lane_no and not job_no:
        return {"error": "Lane No and Job No missing"}
    elif not lane_no:
        return {"error": "Lane No missing"}
    elif not job_no:
        return {"error": "Job No missing"}

    return {"LaneNo": lane_no, "JobNo": job_no}



def parse_transaction_line(line: str, plaza_no: str = "000"):
    if len(line.strip()) < 10:
        return None, False

    # 1️⃣ Cari tarikh & masa dulu
    dt_match = re.search(r'(\d{1,2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2}(?::\d{2})?)', line)
    if not dt_match:
        return None, False

    date_part = dt_match.group(1)
    time_part = dt_match.group(2)

    if len(time_part.split(":")) == 2:
        time_part += ":00"

    date_time = f"{date_part} {time_part}"

    # 2️⃣ Ambil semua digit sebelum tarikh sebagai TrxNo
    before_date = line[:dt_match.start()].strip()

    trx_match = re.match(r'^\d+', before_date)
    if not trx_match:
        return None, False

    trx_no = trx_match.group(0)


    if len(time_part.split(":")) == 2:
        time_part += ":00"
    date_time = f"{date_part} {time_part}"

    start_pos = line.find(f"{date_part} {time_part}")
    if start_pos == -1:
        return None, False
    rest = line[start_pos + len(f"{date_part} {time_part}"):].strip()
    tokens = rest.split()

    origin = fare = card = trn = dtc = pay_mode = "NULL"
    fare_amount = "0.00"
    card_no = mfg = acc_type = vehicle = paid_amount = balance = code = remark = ""

    idx = 0

    if idx < len(tokens) and re.fullmatch(r"\d{3}", tokens[idx]):
        origin = tokens[idx]; idx += 1
    if idx < len(tokens) and re.fullmatch(r"\d{3}", tokens[idx]):
        fare = tokens[idx]; idx += 1

    seq = []
    raw_segment = rest

    def peek():
        return tokens[idx] if idx < len(tokens) else None

    def consume():
        nonlocal idx
        v = tokens[idx]
        idx += 1
        return v

    while peek() and re.fullmatch(r"\d", peek()) and len(seq) < 3:
        seq.append(consume())

    missing_trn = bool(re.search(r'(\d)(\s{2,})(\d)', raw_segment))
    missing_dtc = bool(re.search(r'(\d)(\d)(\s{2,})', raw_segment))
    ends_with_big_space = bool(re.search(r'\d\s{2,}$', raw_segment))

    if len(seq) == 3:
        card, trn, dtc = seq
    elif len(seq) == 2:
        if missing_trn:
            card = seq[0]
            trn = "NULL"
            dtc = seq[1]
        elif missing_dtc or ends_with_big_space:
            card = seq[0]
            trn = seq[1]
            dtc = "NULL"
        else:
            card = "NULL"
            trn = seq[0]
            dtc = seq[1]
    elif len(seq) == 1:
        card = "NULL"
        trn = "NULL"
        dtc = seq[0]
    else:
        card = trn = dtc = "NULL"

    if idx < len(tokens) and tokens[idx].isalpha():
        pay_mode = tokens[idx]; idx += 1

    if idx < len(tokens) and is_valid_money_format(tokens[idx]):
        fare_amount = normalize_money(tokens[idx]); idx += 1

    if idx < len(tokens):
        card_no = tokens[idx]; idx += 1

    if idx < len(tokens):
        tag_id = tokens[idx]; idx += 1
    else:
        tag_id = "NULL"

    # Ambil semua token selebihnya selepas tag_id
    remaining_tokens = tokens[idx:]

    # Kumpulkan semua nilai wang dalam baki token
    money_vals = []
    for tok in remaining_tokens:
        if is_valid_money_format(tok):
            money_vals.append(normalize_money(tok))

    # Tentukan PaidAmount dan Balance berdasarkan dua nilai wang terakhir
    if len(money_vals) >= 2:
        paid_amount = money_vals[-2]
        balance = money_vals[-1]
    elif len(money_vals) == 1:
        paid_amount = money_vals[0]
        balance = "0.00"
    else:
        paid_amount = "0.00"
        balance = "0.00"

    row = [
        trx_no, date_time,
        origin, fare, plaza_no,
        card, trn, dtc,
        pay_mode, fare_amount,
        card_no, tag_id,
        paid_amount, balance,
    ]

    if origin == "NULL" or fare == "NULL":
        return None, False

    return row, True

def clean_header(col):
    col = str(col)
    col = re.sub(r"\s+", "", col)
    col = re.sub(r"&|/|\(|\)|\\n|RM", "", col, flags=re.IGNORECASE)
    return col

def parse_date_ranges(start_date, end_date, filter_6am=True):
    if start_date and not end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d") + (timedelta(hours=6) if filter_6am else timedelta())
        end_dt = start_dt + timedelta(days=1)
    elif start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d") + (timedelta(hours=6) if filter_6am else timedelta())
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + (timedelta(hours=6) if filter_6am else timedelta(days=1))
    else:
        now = datetime.now()
        start_dt = (now - timedelta(days=2)).replace(hour=6 if filter_6am else 0, minute=0, second=0)
        end_dt = now.replace(hour=6 if filter_6am else 23, minute=0 if filter_6am else 59, second=0 if filter_6am else 59)
    return start_dt, end_dt

# === /upload ENDPOINT ===
@app.post("/upload")
async def upload_csv(files: list[UploadFile] = File(...), preview: bool = Query(False)):
    try:
        dfs = []
        for file in files:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(await file.read())
                tmp_path = tmp.name

            temp = pd.read_csv(tmp_path, header=None, dtype=str, encoding='utf-8', skip_blank_lines=True)
            row6 = temp.iloc[5].fillna("")
            row7 = temp.iloc[6].fillna("")

            combined_header = []
            for h6, h7 in zip(row6, row7):
                combined_header.append(str(h7).strip() or str(h6))

            temp.columns = combined_header
            temp = temp.iloc[7:].reset_index(drop=True)
            dfs.append(temp)
            os.remove(tmp_path)

        df = pd.concat(dfs, ignore_index=True)
        df.columns = [clean_header(c) for c in df.columns]

        drop_cols = ["Exit", "Class", "Exceptional"]
        df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

        final_cols = [
            "TrxNo", "PlazaNo", "LaneNo", "EntryPlaza", "JobNo", "TransactionDateTime",
            "Trx", "AVC", "PaymentMode", "FareAmount", "MfgNoTagID", "PaidAmount",
            "Balance", "AccountType", "VehicleNo", "Code", "Remark", "PenaltyCode"
        ]
        final_cols = [c for c in final_cols if c in df.columns]
        df = df[final_cols]

        money_cols = ["FareAmount", "PaidAmount", "Balance"]
        for col in money_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .replace(r"[^\d.\-]", "", regex=True)
                    .replace(r"^\s*$", np.nan, regex=True)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "TransactionDateTime" in df.columns:
            df["TransactionDateTime"] = (
                df["TransactionDateTime"]
                .astype(str)
                .str.replace(r"\s*(AM|PM)", "", regex=True)
                .str.strip()
            )
            df["TransactionDateTime"] = pd.to_datetime(df["TransactionDateTime"], dayfirst=True, errors="coerce")
            df["TransactionDateTime"] = df["TransactionDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # ✅ Gantikan applymap → map
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df.replace(
            ["", " ", "NaN", "nan", "NULL", "null", "None", "N/A", "-", "--"],
            np.nan,
            inplace=True,
        )

        non_numeric_cols = [c for c in df.columns if c not in money_cols]
        df[non_numeric_cols] = df[non_numeric_cols].fillna("NULL")

        if preview:
            return {
                "status": "preview",
                "rows": len(df),
                "message": f"{len(df)} rekod dalam fail (tiada deduplication sebelum insert)",
                "download": None
            }

        # Dapatkan ID terakhir
        with engine.begin() as conn:
            result = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM public.sde22"))
            current_max_id = result.scalar()

        df = df.copy()
        df.insert(0, "id", range(current_max_id + 1, current_max_id + 1 + len(df)))

        for col in ["OriginPlaza", "CardNo"]:
            if col not in df.columns:
                df[col] = "NULL"

        # INSERT SEMUA
        df.to_sql("sde22", engine, schema="public", if_exists="append", index=False)

        # DEDUPLICATION + KIRA
        with engine.begin() as conn:
            total_before = len(df)
            result = conn.execute(text("""
                WITH duplicates AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY "TrxNo", "TransactionDateTime", "OriginPlaza", "EntryPlaza",
                                            "PlazaNo", "Trx", "AVC", "PaymentMode", "FareAmount",
                                            "MfgNoTagID", "PaidAmount", "Balance", "AccountType", "VehicleNo",
                                            "Code", "Remark", "PenaltyCode", "LaneNo", "JobNo", "CardNo"
                               ORDER BY id
                           ) AS rn
                    FROM public.sde22
                )
                DELETE FROM public.sde22
                WHERE id IN (SELECT id FROM duplicates WHERE rn > 1)
                RETURNING id;
            """))
            deleted_count = len(result.fetchall())
            rows_inserted = total_before - deleted_count

        # ✅ INI HARUS DI DALAM try — TIDAK BOLEH DI LUAR!
        return {
            "status": "success",
            "rows": rows_inserted,
            "duplicate": deleted_count,
            "message": f"Berjaya: {rows_inserted} rekod baru disimpan, {deleted_count} duplikat dihapus.",
            "download": None
        }

    except Exception as e:
        import traceback
        print("❌ ERROR:", traceback.format_exc())
        return {"status": "error", "message": str(e)}


# === /wtng ENDPOINT ===
@app.get("/wtng")
def get_wtng_data(
    start_date: str = None,
    end_date: str = None,
    plazas: str = Query(None, description="Senarai PlazaNo dipisah koma"),
    payment_modes: str = Query(None, description="Senarai PaymentMode dipisah koma"),
    batch_size: int = 1000
):
    try:
        start_dt_normal, end_dt_normal = parse_date_ranges(start_date, end_date, filter_6am=False)
        start_dt_special, end_dt_special = parse_date_ranges(start_date, end_date, filter_6am=True)

        plaza_list = [p.strip() for p in plazas.split(",")] if plazas else []
        pm_list = [p.strip() for p in payment_modes.split(",")] if payment_modes else []

        query_template = """
            SELECT
                "id", "Trx", "TrxNo", "PlazaNo", "EntryPlaza", "LaneNo",
                "TransactionDateTime", "PaidAmount", "MfgNoTagID",
                "FareAmount", "VehicleNo", "PaymentMode",
                "Balance", "Code", "PenaltyCode", "Remark", "AVC",
                "OriginPlaza", "CardNo"
            FROM public.sde22
            WHERE 1=1
        """

        query_special = query_template + """
            AND "PaymentMode" IN ('TNG', 'CSC', 'ABT')
            AND "TransactionDateTime" >= :start_special
            AND "TransactionDateTime" < :end_special
        """
        query_normal = query_template + """
            AND "PaymentMode" NOT IN ('TNG', 'CSC', 'ABT')
            AND "TransactionDateTime" >= :start_normal
            AND "TransactionDateTime" < :end_normal
        """

        params_special = {"start_special": start_dt_special, "end_special": end_dt_special}
        params_normal = {"start_normal": start_dt_normal, "end_normal": end_dt_normal}

        if plaza_list:
            query_special += " AND \"PlazaNo\" = ANY(:plazas)"
            query_normal += " AND \"PlazaNo\" = ANY(:plazas)"
            params_special["plazas"] = plaza_list
            params_normal["plazas"] = plaza_list

        if pm_list:
            query_special += " AND \"PaymentMode\" = ANY(:payment_modes)"
            query_normal += " AND \"PaymentMode\" = ANY(:payment_modes)"
            params_special["payment_modes"] = pm_list
            params_normal["payment_modes"] = pm_list

        offset = 0
        dfs_special = []
        while True:
            params_special.update({"limit": batch_size, "offset": offset})
            batch_df = pd.read_sql(
                text(query_special + " ORDER BY \"TransactionDateTime\" LIMIT :limit OFFSET :offset"),
                engine,
                params=params_special
            )
            if batch_df.empty:
                break
            dfs_special.append(batch_df)
            offset += batch_size
        df_special = pd.concat(dfs_special, ignore_index=True) if dfs_special else pd.DataFrame()

        offset = 0
        dfs_normal = []
        while True:
            params_normal.update({"limit": batch_size, "offset": offset})
            batch_df = pd.read_sql(
                text(query_normal + " ORDER BY \"TransactionDateTime\" LIMIT :limit OFFSET :offset"),
                engine,
                params=params_normal
            )
            if batch_df.empty:
                break
            dfs_normal.append(batch_df)
            offset += batch_size
        df_normal = pd.concat(dfs_normal, ignore_index=True) if dfs_normal else pd.DataFrame()

        df = pd.concat([df_special, df_normal], ignore_index=True)
        if not df.empty and "TransactionDateTime" in df.columns:
            df = df.sort_values("TransactionDateTime").reset_index(drop=True)
        else:
            df["TransactionDateTime"] = pd.NaT

        for col in ["OriginPlaza", "CardNo"]:
            if col not in df.columns:
                df[col] = "NULL"
            else:
                df[col] = df[col].fillna("NULL")
        df = df.fillna("NULL")

        chart_entry = df.groupby("EntryPlaza").size().reset_index(name="total").to_dict(orient="records") if not df.empty else []
        chart_plaza = df.groupby("PlazaNo")["PaidAmount"].sum().reset_index().to_dict(orient="records") if not df.empty else []
        chart_avc = df.groupby("AVC").size().reset_index(name="total").to_dict(orient="records") if "AVC" in df.columns and not df.empty else []

        return {
            "status": "success",
            "count": len(df),
            "data": df.to_dict(orient="records"),
            "chart_entry": chart_entry,
            "chart_plaza": chart_plaza,
            "chart_avc": chart_avc
        }

    except Exception:
        import traceback
        print("❌ ERROR in /wtng:\n", traceback.format_exc())
        return {"status": "error", "message": "Ralat semasa ambil data WTNG (stable)."}


# === /traffic-summary ENDPOINT ===
@app.get("/traffic-summary")
def get_traffic_summary(
    start_date: str = None,
    end_date: str = None,
    plazas: str = Query(None, description="Senarai EntryPlaza dipisah koma")
):
    try:
        start_dt, end_dt = parse_date_ranges(start_date, end_date, filter_6am=False)

        query = """
            SELECT
                "Trx" AS class,
                "EntryPlaza",
                COUNT(*) AS total_traffic,
                SUM(COALESCE("PaidAmount",0)::numeric) AS total_paid
            FROM public.sde22
            WHERE "TransactionDateTime" >= :start
              AND "TransactionDateTime" < :end
        """
        params = {"start": start_dt, "end": end_dt}

        if plazas:
            plaza_list = [p.strip() for p in plazas.split(",") if p.strip()]
            placeholders = ", ".join([f":p{i}" for i in range(len(plaza_list))])
            for i, val in enumerate(plaza_list):
                params[f"p{i}"] = val
            query += f' AND "EntryPlaza" IN ({placeholders})'

        query += ' GROUP BY "Trx", "EntryPlaza" ORDER BY "Trx", "EntryPlaza"'

        df = pd.read_sql(text(query), engine, params=params)
        if df.empty:
            return {"status": "success", "data": [], "columns": []}

        traffic_pivot = df.pivot_table(
            index="class", columns="EntryPlaza", values="total_traffic", aggfunc="sum", fill_value=0
        )
        paid_pivot = df.pivot_table(
            index="class", columns="EntryPlaza", values="total_paid", aggfunc="sum", fill_value=0
        )

        all_plazas = sorted(df["EntryPlaza"].unique())
        table_data = []

        for cls in traffic_pivot.index:
            row = {"class": cls}
            for plaza in all_plazas:
                row[f"{plaza}_traffic"] = int(traffic_pivot.loc[cls].get(plaza, 0))
                row[f"{plaza}_paid"] = float(paid_pivot.loc[cls].get(plaza, 0))
            table_data.append(row)

        return {"status": "success", "data": table_data, "columns": all_plazas}

    except Exception:
        import traceback
        print("❌ ERROR in /traffic-summary:\n", traceback.format_exc())
        return {"status": "error", "message": "Ralat semasa ambil traffic summary."}


# === /upload-pdf ENDPOINT ===
@app.post("/upload-pdf")
async def upload_pdf(files: list[UploadFile] = File(...), preview: bool = Query(False)):
    try:
        all_rows = []

        for f in files:
            if f.content_type != "application/pdf":
                raise HTTPException(status_code=400, detail="Hanya terima PDF")
            pdf_bytes = await f.read()
            text_all = ""
            try:
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text_all += page_text + "\n"
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Gagal baca PDF: {str(e)}")

            plaza_no = extract_plaza_no_from_text(text_all)
            header_info = extract_lane_job_from_text(text_all)
            if "error" in header_info:
                raise HTTPException(status_code=400, detail=header_info["error"])

            lane_no = header_info["LaneNo"]
            job_no = header_info["JobNo"]


            candidate_lines = clean_pdf_lines(text_all)

            for line in candidate_lines:
              row, is_valid = parse_transaction_line(line, plaza_no=plaza_no)
              if is_valid:
                  # 🔍 DEBUG: Paparkan baris asal & hasil parsing
                  #print(f"RAW LINE: {line.strip()}")
                  #print(f"PARSED ROW (len={len(row)}): {row}")
                  all_rows.append(row)

        if not all_rows:
            raise HTTPException(status_code=404, detail="Tiada transaksi berjaya di-parse")

        headers = [
            "TrxNo", "TransactionDateTime", "OriginPlaza", "EntryPlaza", "PlazaNo",
            "Trx", "AVC", "PaymentMode", "FareAmount", "MfgNoTagID", "PaidAmount", "Balance",
            "AccountType", "VehicleNo", "Code", "Remark", "PenaltyCode", "LaneNo", "JobNo", "CardNo"
        ]
        df = pd.DataFrame([
            [
                row[0],                 # TrxNo
                row[1],                 # TransactionDateTime
                row[2],                 # OriginPlaza
                row[3],                 # EntryPlaza
                row[4],                 # PlazaNo
                row[6],                 # Trx
                "NULL",                 # AVC
                row[8],                 # PaymentMode
                row[9],                 # FareAmount
                row[11],                # MfgNoTagID
                row[12],                # PaidAmount
                row[13],                # Balance
                "NULL",                 # AccountType
                "NULL",                 # VehicleNo
                "NULL",                 # Code
                "NULL",                 # Remark
                "NULL",                 # PenaltyCode
                lane_no,                # LaneNo
                job_no,                 # JobNo
                row[10]                 # CardNo
            ]
            for row in all_rows
        ], columns=headers)

        if "TransactionDateTime" in df.columns:
            df["TransactionDateTime"] = pd.to_datetime(
                df["TransactionDateTime"],
                format="%d/%m/%Y %H:%M:%S",
                errors="coerce"
            )
            df["TransactionDateTime"] = df["TransactionDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # 🔴 TIADA PEMBANDINGAN DENGAN DATA SEDIA ADA — terus insert semua

        if preview:
            return {
                "status": "preview",
                "rows": len(df),
                "message": f"{len(df)} rekod dalam PDF (tiada deduplication sebelum insert)",
                "download": None
            }

        # 🔴 Assign ID baru
        with engine.begin() as conn:
            result = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM public.sde22"))
            current_max_id = result.scalar()

        df = df.copy()
        df.insert(0, "id", range(current_max_id + 1, current_max_id + 1 + len(df)))

        # ===========================
        # DEBUG: cari row numeric problem
        # ===========================
        money_cols = ["FareAmount", "PaidAmount", "Balance"]
        for col in money_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")  # string kosong → NaN
            df[col] = df[col].fillna(0)  # NaN → 0



        # 🔴 INSERT SEMUA
        df.to_sql("sde22", engine, schema="public", if_exists="append", index=False)

        # ✅ DEDUPLICATION + KIRA BILANGAN
        with engine.begin() as conn:
            total_before = len(df)
            result = conn.execute(text("""
                WITH duplicates AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY "TrxNo", "TransactionDateTime", "OriginPlaza", "EntryPlaza",
                                            "PlazaNo", "Trx", "AVC", "PaymentMode", "FareAmount",
                                            "MfgNoTagID", "PaidAmount", "Balance", "AccountType", "VehicleNo",
                                            "Code", "Remark", "PenaltyCode", "LaneNo", "JobNo", "CardNo"
                               ORDER BY id
                           ) AS rn
                    FROM public.sde22
                )
                DELETE FROM public.sde22
                WHERE id IN (SELECT id FROM duplicates WHERE rn > 1)
                RETURNING id;
            """))
            deleted_count = len(result.fetchall())
            rows_inserted = total_before - deleted_count

        # ✅ SEMUA return SUCCESS mesti di dalam try
        return {
            "status": "success",
            "rows": rows_inserted,          # ✅ BETUL
            "duplicate": deleted_count,      # ✅ BETUL
            "message": f"PDF: {rows_inserted} rekod baru disimpan, {deleted_count} duplikat dihapus.",
            "download": None
        }

    except Exception as e:
        import traceback
        print("❌ ERROR in /upload-pdf:", traceback.format_exc())
        return {"status": "error", "message": str(e)}

#=== PARSE ENTRY PDF ===

def parse_entry_line(line: str):
    if not line or len(line.strip()) < 10:
        return {}, False

    line = re.sub(r"\s+", " ", line.strip())

    # Tolak baris header / event
    reject_keywords = [
        "INDIVIDUAL", "TRANSACTIONS", "ENTRY",
        "TOTAL", "PAGE", "PSG", "NO CARD",
        "NO OBU", "EXCEPTION", "EVENT"
    ]
    if any(k in line.upper() for k in reject_keywords):
        return {}, False

    # Contoh ENTRY sebenar:
    # 1 01/01/2026 06:08:09 1 CSC 601464001111312197 1354594255
    pattern = re.compile(
        r"""
        ^(?P<TrxNo>\d+)\s+
        (?P<Date>\d{2}/\d{2}/\d{4})\s+
        (?P<Time>\d{2}:\d{2}:\d{2})\s+
        (?P<Class>\d+)\s+
        (?P<Mode>[A-Za-z0-9]+)
        (?:\s+(?P<CardNo>\d+))?
        (?:\s+(?P<TagID>\d+))?
        """,
        re.VERBOSE
    )

    m = pattern.match(line)
    if not m:
        return {}, False

    # 🔒 RULE PALING PENTING
    trx_class = m.group("Class")

    # Class wajib ada
    if not trx_class:
        return {}, False


    payment_mode = m.group("Mode")
    allowed_modes = {"CSC", "ABT", "ABTC", "TNG", "ENTRY", "RFID"}

    # Mode wajib sah
    if not payment_mode or payment_mode not in allowed_modes:
        return {}, False


    # Kalau Class wujud dan Mode = CSC → tukar jadi ENTRYCSC
    if trx_class and payment_mode == "CSC":
        payment_mode = "ENTRYCSC"


    trx_datetime = f"{m.group('Date')} {m.group('Time')}"

    row = {
        "TrxNo": int(m.group("TrxNo")),
        "TransactionDateTime": trx_datetime,
        "Trx": trx_class,
        "PaymentMode": payment_mode,
        "CardNo": m.group("CardNo") or "NULL",
        "MfgNoTagID": m.group("TagID") or "NULL",
        "EntryPlaza": "NULL",
        "OriginPlaza": "NULL"
    }

    return row, True


@app.post("/entry-pdf")
async def entry_pdf(files: list[UploadFile] = File(...), preview: bool = Query(False)):
    try:
        all_rows = []

        for f in files:
            if f.content_type != "application/pdf":
                raise HTTPException(status_code=400, detail="Hanya terima PDF")

            pdf_bytes = await f.read()
            text_all = ""

            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_all += page_text + "\n"

            plaza_no = extract_plaza_no_from_text(text_all)
            header_info = extract_lane_job_from_text(text_all)

            lane_no = header_info.get("LaneNo")
            job_no  = header_info.get("JobNo")

            lines = clean_pdf_lines(text_all)

            for line in lines:
                row, is_valid = parse_entry_line(line)
                if not is_valid:
                    continue

                all_rows.append([
                    row["TrxNo"],
                    plaza_no,
                    lane_no,
                    row.get("EntryPlaza", "NULL"),
                    job_no,
                    row["TransactionDateTime"],
                    row.get("Trx"),           # Trx = Class sebagai string
                    "NULL",                     # AVC
                    row.get("PaymentMode"),
                    0.00,                       # FareAmount
                    row.get("MfgNoTagID", "NULL"),
                    0.00,                       # PaidAmount
                    0.00,                       # Balance
                    "NULL", "NULL", "NULL", "NULL", "NULL",
                    row.get("OriginPlaza", "NULL"),
                    row.get("CardNo", "NULL")
                ])

        if not all_rows:
            raise HTTPException(status_code=400, detail="Tiada transaksi ENTRY sah")

        columns = [
            "TrxNo","PlazaNo","LaneNo","EntryPlaza","JobNo",
            "TransactionDateTime","Trx","AVC","PaymentMode",
            "FareAmount","MfgNoTagID","PaidAmount","Balance",
            "AccountType","VehicleNo","Code","Remark","PenaltyCode",
            "OriginPlaza","CardNo"
        ]

        df = pd.DataFrame(all_rows, columns=columns)

        # Convert datetime
        df["TransactionDateTime"] = pd.to_datetime(
            df["TransactionDateTime"],
            format="%d/%m/%Y %H:%M:%S",
            errors="coerce"
        )

        if preview:
            return {
                "status": "preview",
                "rows": len(df),
                "message": "ENTRY PDF preview (tanpa insert)"
            }

        # Assign ID baru
        with engine.begin() as conn:
            max_id = conn.execute(
                text("SELECT COALESCE(MAX(id),0) FROM public.sde22")
            ).scalar()

        df.insert(0, "id", range(max_id + 1, max_id + 1 + len(df)))

        # Insert semua dulu
        df.to_sql(
            "sde22",
            engine,
            schema="public",
            if_exists="append",
            index=False
        )

        # 🔹 Deduplication gaya upload-pdf
        with engine.begin() as conn:
            result = conn.execute(text("""
                WITH duplicates AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY "TrxNo", "TransactionDateTime", "OriginPlaza", "EntryPlaza",
                                            "PlazaNo", "Trx", "AVC", "PaymentMode", "FareAmount",
                                            "MfgNoTagID", "PaidAmount", "Balance", "AccountType", "VehicleNo",
                                            "Code", "Remark", "PenaltyCode", "LaneNo", "JobNo", "CardNo"
                               ORDER BY id
                           ) AS rn
                    FROM public.sde22
                )
                DELETE FROM public.sde22
                WHERE id IN (SELECT id FROM duplicates WHERE rn > 1)
                RETURNING id;
            """))
            deleted_count = len(result.fetchall())
            rows_inserted = len(df) - deleted_count

        return {
            "status": "success",
            "rows": rows_inserted,
            "duplicate": deleted_count,
            "message": f"{rows_inserted} ENTRY transaksi disimpan, {deleted_count} duplikat dihapus."
        }

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {"status": "error", "message": str(e)}



@app.post("/refresh-payment-summary")
async def refresh_payment_summary():

    try:
        with engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW public.date_payment_summary_cashless_per_plaza;"))

        with engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW public.date_payment_summary_with_jobs;"))

        with engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW public.date_payment_summary_entrycsc;"))



        return {"status": "success", "message": "Both materialized views refreshed"}
    except Exception as e:
        import traceback
        print("❌ ERROR refreshing views:", traceback.format_exc())
        return {"status": "error", "message": str(e)}

# ---------------- UTILITY -------------------
def apply_filters(query: str, params: dict,
                  plaza: list[str] | str | None = None,
                  payment: list[str] | str | None = None,
                  trx: list[str] | str | None = None):
    # Wrap single string as
    if plaza:
        if isinstance(plaza, str):
            plaza = [plaza]
        query += ' AND "PlazaNo" = ANY(:plaza)'
        params["plaza"] = plaza

    if payment:
        if isinstance(payment, str):
            payment = [payment]
        query += ' AND "PaymentMode" = ANY(:payment)'
        params["payment"] = payment

    if trx:
        if isinstance(trx, str):
            trx = [trx]
        query += ' AND "Trx" = ANY(:trx)'
        params["trx"] = trx

    return query, params


def parse_date_range(start_date: str, end_date: str):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    return start_dt, end_dt

def build_time_params(start_dt: datetime):
    return {
        "start_6am": start_dt.replace(hour=6, minute=0, second=0),
        "end_6am": (start_dt + timedelta(days=1)).replace(hour=6),
        "start_midnight": start_dt.replace(hour=0, minute=0, second=0),
        "end_midnight": (start_dt + timedelta(days=1)).replace(hour=0),
    }

def build_range_params(start_dt: datetime, end_dt: datetime):
    return {
        "start_6am": start_dt.replace(hour=6, minute=0, second=0),
        "end_6am": end_dt.replace(hour=6, minute=0, second=0),

        "start_midnight": start_dt.replace(hour=0, minute=0, second=0),
        "end_midnight": end_dt.replace(hour=0, minute=0, second=0),
    }


# ---------------- ENDPOINTS ----------------
@app.get("/trx-per-plaza")
def trx_per_plaza(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "PlazaNo",
               COUNT(DISTINCT ("CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount")) AS total_trx
        FROM public.sde22
        WHERE "FareAmount" > 0
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    """

    query, params = apply_filters(query, params, plaza, payment, trx)
    query += ' GROUP BY "PlazaNo" ORDER BY "PlazaNo"'

    df = pd.read_sql(text(query), engine, params=params)

    return {
        "status": "success",
        "chart_plaza": df.to_dict("records")
    }


@app.get("/trx-by-payment-mode")
def trx_by_payment_mode(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "PaymentMode",
               COUNT(DISTINCT ("CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount")) AS total_trx
        FROM public.sde22
        WHERE "FareAmount" > 0
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    """

    query, params = apply_filters(query, params, plaza, payment, trx)
    query += ' GROUP BY "PaymentMode" ORDER BY "PaymentMode"'

    df = pd.read_sql(text(query), engine, params=params)

    return {
        "status": "success",
        "chart_payment": df.to_dict("records")
    }

@app.get("/trx-by-plaza-bar")
def trx_by_plaza_bar(
    start_date: str,
    end_date: str,
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "PlazaNo",
               COUNT(DISTINCT ("CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount")) AS total_trx
        FROM public.sde22
        WHERE "FareAmount" > 0
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    """

    # We skip plaza filter here (as in original) and only apply payment/trx filters
    query, params = apply_filters(query, params, None, payment, trx)
    query += ' GROUP BY "PlazaNo" ORDER BY "PlazaNo"'

    df = pd.read_sql(text(query), engine, params=params)

    return {"status": "success", "chart_bar": df.to_dict("records")}



@app.get("/trx-by-class")
def trx_by_class(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "Trx",
               COUNT(DISTINCT ("CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount")) AS total_trx
        FROM public.sde22
        WHERE "FareAmount" > 0
          AND "Trx" IN ('1','2','3','4','5')
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    """

    query, params = apply_filters(query, params, plaza, payment, None)
    query += ' GROUP BY "Trx" ORDER BY "Trx"'

    df = pd.read_sql(text(query), engine, params=params)
    return {"status": "success", "chart_class": df.to_dict("records")}


@app.get("/summary")
def summary(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    # Subquery selects only distinct transactions based on 5 key columns
    query = """
        SELECT COUNT(*) AS total_trx,
               SUM("PaidAmount") AS total_paid_amount
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """

    query, params = apply_filters(query, params, plaza, payment, trx)
    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {
        "status": "success",
        "totalTraffic": result["total_trx"],
        "totalPaidAmount": float(result["total_paid_amount"]) if result["total_paid_amount"] else 0
    }

#--------------Payment---------------------

@app.get("/payment-per-plaza")
def payment_per_plaza(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "PlazaNo", SUM("PaidAmount") AS total_payment
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount", "PlazaNo"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """
    query, params = apply_filters(query, params, plaza, payment, trx)
    query += ' GROUP BY "PlazaNo" ORDER BY "PlazaNo"'

    df = pd.read_sql(text(query), engine, params=params)
    return {"status": "success", "chart_plaza": df.to_dict("records")}

@app.get("/payment-by-payment-mode")
def payment_by_payment_mode(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "PaymentMode", SUM("PaidAmount") AS total_payment
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount", "PaymentMode"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """
    query, params = apply_filters(query, params, plaza, payment, trx)
    query += ' GROUP BY "PaymentMode" ORDER BY "PaymentMode"'

    df = pd.read_sql(text(query), engine, params=params)
    return {"status": "success", "chart_payment": df.to_dict("records")}

@app.get("/payment-by-plaza-bar")
def payment_by_plaza_bar(
    start_date: str,
    end_date: str,
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "PlazaNo", SUM("PaidAmount") AS total_payment
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount", "PlazaNo"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND "PlazaNo" IN ('201','202','203','204')
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """
    query, params = apply_filters(query, params, None, payment, trx)
    query += ' GROUP BY "PlazaNo" ORDER BY "PlazaNo"'

    df = pd.read_sql(text(query), engine, params=params)
    return {"status": "success", "chart_bar": df.to_dict("records")}

@app.get("/payment-by-class")
def payment_by_class(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT "Trx", SUM("PaidAmount") AS total_payment
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount", "Trx"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND "Trx" IN ('1','2','3','4','5')
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """

    query, params = apply_filters(query, params, plaza, payment, None)
    query += ' GROUP BY "Trx" ORDER BY "Trx"'

    df = pd.read_sql(text(query), engine, params=params)

    return {"status": "success", "chart_class": df.to_dict("records")}

@app.get("/payment-summary")
def payment_summary(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT SUM("PaidAmount") AS total_payment
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """

    query, params = apply_filters(query, params, plaza, payment, trx)
    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {"status": "success", "totalPayment": float(result["total_payment"] or 0)}


# ---------------- segment ----------------
@app.get("/segment1-2")
def total_entry_exit(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)

    params = {
        "plaza": plaza if plaza else ['201'],
        **time_params
    }

    query = """
    WITH entry_data AS (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('ENTRY','ENTRYCSC')
          AND "PlazaNo" = ANY(:plaza)
          AND (
              ("LaneNo" IN ('M01','M02','M03','M05')
              AND "TransactionDateTime" >= :start_6am
              AND "TransactionDateTime" < :end_6am)
              OR
              ("LaneNo" = 'M04'
              AND "TransactionDateTime" >= :start_midnight
              AND "TransactionDateTime" < :end_midnight)
          )
    ),
    exit_data AS (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('TNG','CSC','RFID','ABT','ABTC')
          AND "FareAmount" <> 0
          AND "PlazaNo" = ANY(:plaza)
          AND (
              ("PaymentMode" IN ('TNG','CSC')
              AND "TransactionDateTime" >= :start_6am
              AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
              AND "TransactionDateTime" >= :start_midnight
              AND "TransactionDateTime" < :end_midnight)
          )
    )
    SELECT
        (SELECT COUNT(*) FROM entry_data) AS total_entry,
        (SELECT COUNT(*) FROM exit_data) AS total_exit,
        (SELECT COUNT(*) FROM entry_data) + (SELECT COUNT(*) FROM exit_data) AS total_trx
    """
    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {
        "status": "success",
        "totalEntry": result["total_entry"],
        "totalExit": result["total_exit"],
        "totalTrafficSegment1": result["total_trx"]
    }

# ---------------- segment3 ----------------
@app.get("/segment3")
def segment_3(
    start_date: str,
    end_date: str
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)

    params = {**time_params}

    query = """
    SELECT COUNT(*) AS total_trx
    FROM (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('TNG','CSC','ABT','ABTC','RFID')
          AND "FareAmount" <> 0
          AND ("EntryPlaza","PlazaNo") IN (
                ('203','201'),
                ('204','201'),
                ('203','202'),
                ('204','202'),
                ('201','203'),
                ('202','203'),
                ('201','204'),
                ('202','204')
          )
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    ) AS distinct_trx;
    """

    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {
        "status": "success",
        "segment": 3,
        "totalTrafficSegment3": int(result["total_trx"])
    }

# ---------------- segment4 ----------------
@app.get("/segment4")
def total_entry_exit(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)

    params = {"plaza": plaza if plaza else ['203'], **time_params}

    query = """
    WITH entry_data AS (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('ENTRY','ENTRYCSC')
          AND "PlazaNo" = ANY(:plaza)
          AND (
              ("LaneNo" IN ('M01','M02','M03','M04','M05','M06','M08')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("LaneNo" = 'M07'
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    ),
    exit_data AS (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('TNG','CSC','RFID','ABT','ABTC')
          AND "FareAmount" <> 0
          AND "PlazaNo" = ANY(:plaza)
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    )
    SELECT
        (SELECT COUNT(*) FROM entry_data) AS total_entry,
        (SELECT COUNT(*) FROM exit_data) AS total_exit,
        (SELECT COUNT(*) FROM entry_data) + (SELECT COUNT(*) FROM exit_data) AS total_trx
    """
    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {
        "status": "success",
        "totalEntry": result["total_entry"],
        "totalExit": result["total_exit"],
        "totalTrafficSegment4": result["total_trx"]
    }

# ---------------- segment6 ----------------
@app.get("/segment6")
def total_entry_exit(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)

    params = {"plaza": plaza if plaza else ['204'], **time_params}

    query = """
    WITH entry_data AS (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('ENTRY','ENTRYCSC')
          AND "PlazaNo" = ANY(:plaza)
          AND (
              ("LaneNo" IN ('M03','M02','M01')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("LaneNo" = 'M04'
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    ),
    exit_data AS (
        SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
        FROM public.sde22
        WHERE "PaymentMode" IN ('TNG','CSC','RFID','ABT','ABTC')
          AND "FareAmount" <> 0
          AND "PlazaNo" = ANY(:plaza)
          AND (
              ("PaymentMode" IN ('TNG','CSC')
               AND "TransactionDateTime" >= :start_6am
               AND "TransactionDateTime" < :end_6am)
              OR
              ("PaymentMode" IN ('ABT','RFID','ABTC')
               AND "TransactionDateTime" >= :start_midnight
               AND "TransactionDateTime" < :end_midnight)
          )
    )
    SELECT
        (SELECT COUNT(*) FROM entry_data) AS total_entry,
        (SELECT COUNT(*) FROM exit_data) AS total_exit,
        (SELECT COUNT(*) FROM entry_data) + (SELECT COUNT(*) FROM exit_data) AS total_trx
    """
    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {
        "status": "success",
        "totalEntry": result["total_entry"],
        "totalExit": result["total_exit"],
        "totalTrafficSegment6": result["total_trx"]
    }


@app.get("/payment-summary")
def payment_summary(
    start_date: str,
    end_date: str,
    plaza: list[str] | str | None = Query(None),
    payment: list[str] | str | None = Query(None),
    trx: list[str] | str | None = Query(None)
):
    start_dt, end_dt = parse_date_range(start_date, end_date)
    time_params = build_range_params(start_dt, end_dt)
    params = {**time_params}

    query = """
        SELECT SUM("PaidAmount") AS total_payment
        FROM (
            SELECT DISTINCT "CardNo", "TransactionDateTime", "MfgNoTagID", "PaidAmount", "FareAmount"
            FROM public.sde22
            WHERE "FareAmount" > 0
              AND (
                  ("PaymentMode" IN ('TNG','CSC')
                   AND "TransactionDateTime" >= :start_6am
                   AND "TransactionDateTime" < :end_6am)
                  OR
                  ("PaymentMode" IN ('ABT','RFID','ABTC')
                   AND "TransactionDateTime" >= :start_midnight
                   AND "TransactionDateTime" < :end_midnight)
              )
        ) AS distinct_trx
    """

    query, params = apply_filters(query, params, plaza, payment, trx)

    df = pd.read_sql(text(query), engine, params=params)
    result = df.to_dict("records")[0]

    return {
        "status": "success",
        "totalPayment": float(result["total_payment"] or 0)
    }


@app.get("/")
def read_root():
    return {"message": "Backend sde22 is running successfully!"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
# 29/1/2026
