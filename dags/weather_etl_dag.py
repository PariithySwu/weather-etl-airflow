import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

RAW_FILE = '/opt/airflow/data/weatherAUS.csv'
STAGING_BASE = '/opt/airflow/data/staging'

# ---------------------------------------------------------
# 1. LOAD (Extract): จำลองดึงข้อมูลรายวัน และจัดการ Data Management
# ---------------------------------------------------------
def extract_data(ds, **kwargs):
    print(f"Extracting data for date: {ds}")
    df = pd.read_csv(RAW_FILE)
    
    # Simulation: จำลองว่ามีข้อมูลเข้ามาแค่วันละ 1 วันตามที่ Airflow รัน (ds)
    df_daily = df[df['Date'] == ds]
    
    if df_daily.empty:
        raise ValueError(f"No data found for {ds}")
    
    # Data Management: ทำ Partitioning แยกโฟลเดอร์ตามปี/เดือน/วัน
    dt = datetime.strptime(ds, '%Y-%m-%d')
    partition_path = f"{STAGING_BASE}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    os.makedirs(partition_path, exist_ok=True)
    
    out_path = f"{partition_path}/raw_weather.parquet"
    df_daily.to_parquet(out_path, index=False)
    print(f"Extracted {len(df_daily)} rows to {out_path}")

# ---------------------------------------------------------
# 2. TRANSFORM: ทำ Data Quality Check และจัดการค่าว่าง
# ---------------------------------------------------------
def transform_data(ds, **kwargs):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    partition_path = f"{STAGING_BASE}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    in_path = f"{partition_path}/raw_weather.parquet"
    
    df = pd.read_parquet(in_path)
    
    # วิธีแก้ที่ 1: Data Pruning (ตัดคอลัมน์ที่ไม่ใช้ออกไป)
    columns_to_keep = ['Date', 'Location', 'MinTemp', 'MaxTemp', 'Rainfall', 'Humidity9am', 'Humidity3pm']
    df = df[columns_to_keep].copy()
    
    # Data Quality 1: ลบแถวที่คีย์หลักว่างเปล่าทิ้ง
    df = df.dropna(subset=['MinTemp', 'MaxTemp', 'Location'])
    
    # วิธีแก้ที่ 2: Data Imputation (เติมค่าว่างด้วยค่าเฉลี่ย)
    df['Humidity9am'] = df['Humidity9am'].fillna(df['Humidity9am'].mean())
    df['Humidity3pm'] = df['Humidity3pm'].fillna(df['Humidity3pm'].mean())
    
    # 🌟 สิ่งที่เพิ่มเข้ามา: ปัดเศษทศนิยมทุกคอลัมน์ที่เป็นตัวเลขให้เหลือ 1 ตำแหน่ง
    df = df.round(1)
    
    # Data Quality 2: ตรวจสอบความสมเหตุสมผลของข้อมูล (Sanity Check)
    df = df[df['MaxTemp'] >= df['MinTemp']]
    df['Rainfall'] = df['Rainfall'].fillna(0)
    df = df[df['Rainfall'] >= 0]
    
    # แปลง Location เป็นตัวพิมพ์เล็ก
    df['Location'] = df['Location'].str.lower()
    
    # เซฟทับไฟล์เดิม ทั้ง Parquet และ CSV
    out_parquet = f"{partition_path}/clean_weather.parquet"
    df.to_parquet(out_parquet, index=False)
    
    out_csv = f"{partition_path}/clean_weather.csv"
    df.to_csv(out_csv, index=False)
    
    print(f"Transformed data. Remaining rows: {len(df)}")

# ---------------------------------------------------------
# 3. INGEST (Load): นำเข้า Database แบบ Star Schema และ Security
# ---------------------------------------------------------
def ingest_data(ds, **kwargs):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    partition_path = f"{STAGING_BASE}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    in_path = f"{partition_path}/clean_weather.parquet"
    
    df = pd.read_parquet(in_path)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_weather_db')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # =========================================================
    # 🌟 IDEMPOTENCY LOGIC: ลบข้อมูลของวันนั้นทิ้งก่อน (Delete Before Insert)
    # =========================================================
    with engine.begin() as conn:
        try:
            # สั่งลบแถวที่มีวันที่ตรงกับตัวแปร ds
            conn.execute(text(f"""DELETE FROM fact_weather WHERE "Date" = '{ds}';"""))
            conn.execute(text(f"""DELETE FROM dim_date WHERE full_date = '{ds}';"""))
            # สำหรับสถานที่ (Location) เราลบทิ้งทั้งหมดก่อนชั่วคราวเพื่อป้องกันรายชื่อเมืองเบิ้ล
            conn.execute(text("""DELETE FROM dim_location;""")) 
        except Exception:
            # หากรันครั้งแรกสุด ตารางอาจจะยังไม่ถูกสร้าง มันจะเกิด Error ลบไม่ได้ ให้เราข้ามไปเลย
            pass
    # =========================================================

    # หลังจากเคลียร์พื้นที่เสร็จแล้ว ก็ทำการ Insert ข้อมูลใหม่ลงไปตามปกติ
    dim_location = df[['Location']].drop_duplicates().rename(columns={'Location': 'location_name'})
    dim_location.to_sql('dim_location', engine, if_exists='append', index=False)
    
    dim_date = pd.DataFrame([{
        'full_date': ds,
        'year': dt.year,
        'month': dt.month,
        'day': dt.day
    }])
    dim_date.to_sql('dim_date', engine, if_exists='append', index=False)
    
    fact_weather = df[['Date', 'Location', 'MinTemp', 'MaxTemp', 'Rainfall', 'Humidity9am', 'Humidity3pm']].copy()
    fact_weather.to_sql('fact_weather', engine, if_exists='append', index=False)
    
    print(f"Ingested {len(fact_weather)} rows into Star Schema with Idempotency.")
# ---------------------------------------------------------
# ตั้งค่า Airflow DAG
# ---------------------------------------------------------
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'weather_pipeline_final',
    default_args=default_args,
    schedule_interval='@daily',       # รันอัตโนมัติทุกวัน (Automated)
    start_date=datetime(2008, 12, 1), # วันแรกของ Dataset
    end_date=datetime(2009, 3, 31),   # <-- วันสิ้นสุด (มีนาคม 2009)
    catchup=True,                     # สั่งให้รันย้อนหลังเพื่อจำลองการทำงานรายวัน
    max_active_runs=1,                # บังคับให้รันทีละ 1 วันเท่านั้น
    tags=['weather', 'etl', 'star_schema'],
) as dag:

    t1 = PythonOperator(task_id='load_extract', python_callable=extract_data)
    t2 = PythonOperator(task_id='transform_clean', python_callable=transform_data)
    t3 = PythonOperator(task_id='ingest_to_db', python_callable=ingest_data)

    t1 >> t2 >> t3