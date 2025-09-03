#!/usr/bin/env python3
import argparse
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from decimal import Decimal, InvalidOperation

ALLOWED_KINDS = {"train", "test", "future"}

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    required = ["date", "kind", "predicted_close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В файле отсутствуют обязательные столбцы: {missing}")

    
    df = df[required]

    
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["kind"] = df["kind"].astype(str).str.strip().str.lower()
  
    df["predicted_close"] = pd.to_numeric(df["predicted_close"], errors="coerce")

    
    before = len(df)
    df = df.dropna(subset=["date", "kind", "predicted_close"])
    df = df[df["kind"].isin(ALLOWED_KINDS)]
    after = len(df)
    dropped = before - after
    if dropped > 0:
        print(f"[WARN] Отброшено строк из-за невалидных/пустых данных: {dropped}")

    return df

def to_decimal_6(x) -> Decimal:
   
    try:
        return Decimal(f"{float(x):.6f}")
    except (InvalidOperation, ValueError, TypeError):
        raise

def upsert_predictions(cur, records):
    sql = """
        INSERT INTO public.predictions (date, kind, predicted_close)
        VALUES %s
        ON CONFLICT (date, kind) DO UPDATE
        SET predicted_close = EXCLUDED.predicted_close,
            imported_at = now();
    """
    execute_values(cur, sql, records, page_size=10000)

def main():
    ap = argparse.ArgumentParser(description="Загрузка predictions.csv в public.predictions")
    ap.add_argument("--file", default=os.path.join(os.path.dirname(__file__), "..", "predictions", "predictions.csv"),
                    help="Путь к predictions.csv (по умолчанию ../predictions/predictions.csv)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=55432) 
    ap.add_argument("--db",   default="stocks")
    ap.add_argument("--user", default="stocks_user")
    ap.add_argument("--password", default="stocks_password")
    args = ap.parse_args()

    csv_path = os.path.abspath(args.file)
    if not os.path.exists(csv_path):
        print(f"[ERROR] Файл не найден: {csv_path}", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"[ERROR] Не удалось прочитать CSV: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        df = normalize_df(df)
    except Exception as e:
        print(f"[ERROR] Невалидные данные: {e}", file=sys.stderr)
        sys.exit(1)

    
    records = []
    for r in df.itertuples(index=False):
        try:
            d = getattr(r, "date")
            k = str(getattr(r, "kind"))
            p = to_decimal_6(getattr(r, "predicted_close"))
            records.append((d, k, p))
        except Exception as e:
            print(f"[WARN] Пропускаю строку из-за ошибки преобразования: {e}")

    if not records:
        print("[WARN] Нет валидных записей для загрузки.")
        sys.exit(0)

    conn = None
    try:
        conn = psycopg2.connect(
            host=args.host, port=args.port, dbname=args.db,
            user=args.user, password=args.password
        )
        conn.autocommit = False
        with conn.cursor() as cur:
            upsert_predictions(cur, records)
        conn.commit()
        print(f"[DONE] Загружено записей: {len(records)}")
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
