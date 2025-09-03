#!/usr/bin/env python3
import argparse
import os
import sys
import glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

TICKERS = ("AAPL", "GOOGL", "AMZN", "MSFT")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # нормализуем имена столбцов: "Adj Close" -> "adj_close", "Date" -> "date", и т.п.
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    required = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В файле отсутствуют обязательные столбцы: {missing}")
    df = df[required]

    # приведение типов
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for col in ["open", "high", "low", "close", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # выкидываем строки с пропусками в ключевых полях
    df = df.dropna(subset=["date", "open", "high", "low", "close", "adj_close", "volume"])
    return df

def discover_files(data_dir: str):
    # ищем файлы вида AAPL.csv, GOOGL.csv, AMZN.csv, MSFT.csv (регистр не важен)
    patterns = []
    for t in TICKERS:
        patterns.append(os.path.join(data_dir, f"{t}.csv"))
        patterns.append(os.path.join(data_dir, f"{t.lower()}.csv"))
        patterns.append(os.path.join(data_dir, f"{t}"))          # на случай, если без .csv
        patterns.append(os.path.join(data_dir, f"{t.lower()}"))
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    # убираем дубликаты и сортируем по имени
    files = sorted(set(files))
    if not files:
        raise FileNotFoundError(
            f"Не найдены файлы {TICKERS} в директории '{data_dir}'. "
            "Ожидаются имена вида AAPL.csv, GOOGL.csv, AMZN.csv, MSFT.csv"
        )
    return files

def extract_ticker_from_filename(path: str) -> str:
    base = os.path.basename(path)
    name = os.path.splitext(base)[0]
    name = name.upper()
    for t in TICKERS:
        if name.startswith(t):
            return t
    raise ValueError(f"Не удалось определить тикер из имени файла: {base}")

def upsert_quotes(cur, records):
    # UPSERT пакетами
    sql = """
        INSERT INTO public.quotes
            (company, date, open, high, low, close, adj_close, volume)
        VALUES %s
        ON CONFLICT (company, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume;
    """
    execute_values(cur, sql, records, page_size=10000)

def load_file(cur, path: str, ticker: str):
    print(f"[INFO] Загружаю {ticker} из {path}")
    df = pd.read_csv(path)
    df = normalize_columns(df)
    df["company"] = ticker
    df = df.sort_values("date")
    records = []
    for r in df[["company","date","open","high","low","close","adj_close","volume"]].itertuples(index=False):
        records.append((
            str(r.company),
            r.date,                    # уже python date
            float(r.open),
            float(r.high),
            float(r.low),
            float(r.close),
            float(r.adj_close),
            int(r.volume),
        ))

    if not records:
        print(f"[WARN] В файле {path} после очистки нет валидных записей, пропускаю.")
        return

    upsert_quotes(cur, records)
    print(f"[OK]   {ticker}: загружено строк: {len(records)}")

def main():
    ap = argparse.ArgumentParser(description="Загрузка CSV котировок в PostgreSQL (таблица public.quotes)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--db",   default="stocks")
    ap.add_argument("--user", default="stocks_user")
    ap.add_argument("--password", default="stocks_password")
    ap.add_argument("--dir",  default=os.path.join(os.path.dirname(__file__), "..", "stoks"),
                    help="Директория с CSV (по умолчанию ../stoks)")
    args = ap.parse_args()

    data_dir = os.path.abspath(args.dir)
    files = discover_files(data_dir)

    conn = None
    try:
        conn = psycopg2.connect(
            host=args.host, port=args.port, dbname=args.db,
            user=args.user, password=args.password
        )
        conn.autocommit = False
        with conn.cursor() as cur:
            for path in files:
                ticker = extract_ticker_from_filename(path)
                load_file(cur, path, ticker)
        conn.commit()
        print("[DONE] Загрузка завершена успешно.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
