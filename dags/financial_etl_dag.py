# ================================================
# financial_etl_dag.py - Apache Airflow DAG
# Project: Airflow ETL Pipeline
# Author: Ramya | Senior Data Engineer
# ================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# ── Default Arguments ──────────────────────────
default_args = {
    'owner':            'ramya',
    'depends_on_past':  False,
    'email':            ['ramyamehta9999@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':   False,
    'retries':          3,
    'retry_delay':      timedelta(minutes=5),
    'start_date':       datetime(2024, 1, 1),
}

# ── DAG Definition ─────────────────────────────
dag = DAG(
    'financial_etl_pipeline',
    default_args=default_args,
    description='Daily Financial ETL Pipeline',
    schedule_interval='0 6 * * *',  # Every day at 6AM
    catchup=False,
    tags=['finance', 'etl', 'production']
)

# ── Task Functions ─────────────────────────────
def extract_task(**context):
    """Extract financial data from source"""
    import pandas as pd
    from faker import Faker
    import random
    import os

    print("📥 [EXTRACT] Starting data extraction...")
    fake = Faker()
    os.makedirs('data', exist_ok=True)

    records = []
    for i in range(10000):
        records.append({
            'transaction_id': f'TXN{i+1:06d}',
            'customer_id':    f'CUST{random.randint(1,1000):04d}',
            'amount':         round(random.uniform(10, 50000), 2),
            'category':       random.choice([
                              'Shopping','Food',
                              'Travel','Healthcare']),
            'date':           fake.date_this_year(),
            'is_fraud':       int(random.random() < 0.05)
        })

    df = pd.DataFrame(records)
    df.to_csv('data/raw_transactions.csv', index=False)
    print(f"✅ [EXTRACT] {len(df):,} records extracted!")
    print(f"📅 Execution date: {context['ds']}")
    return len(df)

def transform_task(**context):
    """Transform and clean financial data"""
    import pandas as pd
    import os

    print("⚙️ [TRANSFORM] Starting transformation...")
    os.makedirs('data', exist_ok=True)

    df = pd.read_csv('data/raw_transactions.csv')
    print(f"📥 Loaded {len(df):,} records")

    # Clean data
    df.dropna(inplace=True)

    # Add risk score
    df['risk_score'] = df['amount'].apply(
        lambda x: 'HIGH' if x > 10000
        else 'MEDIUM' if x > 5000
        else 'LOW'
    )

    # Add processed timestamp
    df['processed_at'] = datetime.now().strftime(
                         '%Y-%m-%d %H:%M:%S')

    # Fraud summary
    fraud_count = df['is_fraud'].sum()
    print(f"🚨 Fraud detected: {fraud_count:,} transactions")
    print(f"📊 Risk breakdown:")
    print(df['risk_score'].value_counts())

    df.to_csv('data/processed_transactions.csv', index=False)
    print("✅ [TRANSFORM] Transformation complete!")
    return fraud_count

def load_task(**context):
    """Load processed data to database"""
    import pandas as pd
    import sqlite3
    import os

    print("💾 [LOAD] Starting data load...")
    os.makedirs('data', exist_ok=True)

    df = pd.read_csv('data/processed_transactions.csv')

    conn = sqlite3.connect('data/financial_warehouse.db')
    df.to_sql('transactions', conn,
              if_exists='replace', index=False)

    # Verify
    count = pd.read_sql(
        "SELECT COUNT(*) as total FROM transactions",
        conn).iloc[0]['total']
    print(f"✅ [LOAD] {count:,} records loaded to DB!")

    # Summary report
    summary = pd.read_sql("""
        SELECT 
            COUNT(*) as total,
            SUM(is_fraud) as fraud_count,
            ROUND(AVG(amount), 2) as avg_amount,
            MAX(amount) as max_amount
        FROM transactions
    """, conn)
    print("\n📊 Pipeline Summary:")
    print(summary.to_string(index=False))
    conn.close()
    print("✅ [LOAD] Pipeline complete!")

def quality_check_task(**context):
    """Data quality validation"""
    import pandas as pd

    print("🔍 [QUALITY] Running data quality checks...")
    df = pd.read_csv('data/processed_transactions.csv')

    checks = {
        'total_records':    len(df),
        'null_values':      df.isnull().sum().sum(),
        'duplicate_ids':    df['transaction_id'].duplicated().sum(),
        'fraud_rate_%':     round(df['is_fraud'].mean()*100, 2),
        'avg_amount':       round(df['amount'].mean(), 2)
    }

    print("\n✅ Quality Check Results:")
    for check, value in checks.items():
        status = "✅" if value == 0 or check not in [
                 'null_values','duplicate_ids'] else "❌"
        print(f"  {status} {check}: {value}")

    if checks['null_values'] > 0:
        raise ValueError("❌ NULL values found!")
    if checks['duplicate_ids'] > 0:
        raise ValueError("❌ Duplicate IDs found!")

    print("\n✅ [QUALITY] All checks passed!")
    return checks

# ── Define Tasks ───────────────────────────────
t1_extract = PythonOperator(
    task_id='extract_financial_data',
    python_callable=extract_task,
    provide_context=True,
    dag=dag
)

t2_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

t3_quality = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check_task,
    provide_context=True,
    dag=dag
)

t4_load = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)

# ── Task Dependencies (Pipeline Order) ─────────
# Extract → Transform → Quality Check → Load
t1_extract >> t2_transform >> t3_quality >> t4_load