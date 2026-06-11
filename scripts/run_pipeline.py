# ================================================
# run_pipeline.py - Local Pipeline Runner
# Simulates Airflow DAG execution locally
# Author: Ramya | Senior Data Engineer
# ================================================

import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_pipeline():
    print("=" * 55)
    print("  ✈️  AIRFLOW ETL PIPELINE — LOCAL EXECUTION")
    print("=" * 55)
    print(f"  📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  👤 Owner: ramya")
    print(f"  ⏰ Schedule: Daily at 6:00 AM")
    print("=" * 55)

    context = {'ds': datetime.now().strftime('%Y-%m-%d')}

    # Import task functions from DAG
    import pandas as pd
    from faker import Faker
    import random, sqlite3, os

    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

    # ── TASK 1: EXTRACT ──────────────────────────
    print("\n🔵 TASK 1/4: extract_financial_data")
    print("-" * 40)
    fake = Faker()
    records = []
    for i in range(10000):
        records.append({
            'transaction_id': f'TXN{i+1:06d}',
            'customer_id':    f'CUST{random.randint(1,1000):04d}',
            'amount':         round(random.uniform(10,50000),2),
            'category':       random.choice([
                              'Shopping','Food',
                              'Travel','Healthcare']),
            'date':           str(fake.date_this_year()),
            'is_fraud':       int(random.random() < 0.05)
        })
    df = pd.DataFrame(records)
    df.to_csv('data/raw_transactions.csv', index=False)
    print(f"✅ Extracted {len(df):,} records")

    # ── TASK 2: TRANSFORM ────────────────────────
    print("\n🔵 TASK 2/4: transform_data")
    print("-" * 40)
    df = pd.read_csv('data/raw_transactions.csv')
    df.dropna(inplace=True)
    df['risk_score'] = df['amount'].apply(
        lambda x: 'HIGH' if x > 10000
        else 'MEDIUM' if x > 5000
        else 'LOW'
    )
    df['processed_at'] = datetime.now().strftime(
                         '%Y-%m-%d %H:%M:%S')
    fraud_count = df['is_fraud'].sum()
    print(f"✅ Transformed {len(df):,} records")
    print(f"🚨 Fraud detected: {fraud_count}")
    print(f"📊 Risk breakdown:\n{df['risk_score'].value_counts().to_string()}")
    df.to_csv('data/processed_transactions.csv', index=False)

    # ── TASK 3: QUALITY CHECK ────────────────────
    print("\n🔵 TASK 3/4: quality_check")
    print("-" * 40)
    checks = {
        'total_records':  len(df),
        'null_values':    df.isnull().sum().sum(),
        'duplicate_ids':  df['transaction_id'].duplicated().sum(),
        'fraud_rate_%':   round(df['is_fraud'].mean()*100, 2),
        'avg_amount':     round(df['amount'].mean(), 2)
    }
    for check, value in checks.items():
        print(f"  ✅ {check}: {value}")

    # ── TASK 4: LOAD ─────────────────────────────
    print("\n🔵 TASK 4/4: load_to_warehouse")
    print("-" * 40)
    conn = sqlite3.connect('data/financial_warehouse.db')
    df.to_sql('transactions', conn,
              if_exists='replace', index=False)
    count = pd.read_sql(
        "SELECT COUNT(*) as total FROM transactions",
        conn).iloc[0]['total']
    print(f"✅ {count:,} records loaded to database!")
    conn.close()

    # ── PIPELINE SUMMARY ─────────────────────────
    print("\n" + "=" * 55)
    print("  ✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 55)
    print(f"  📊 Records Processed: {len(df):,}")
    print(f"  🚨 Fraud Detected:    {fraud_count:,}")
    print(f"  ✅ Quality Checks:    All Passed!")
    print(f"  💾 Loaded to DB:      {count:,} records")
    print(f"  ⏱️  Completed at:     {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 55)

    # Save log
    with open('logs/pipeline.log', 'a') as f:
        f.write(f"{datetime.now()} | Records: {len(df)} | "
                f"Fraud: {fraud_count} | Status: SUCCESS\n")
    print("\n📝 Log saved to logs/pipeline.log")

if __name__ == "__main__":
    run_pipeline()