# 🌦️ Automated Weather ETL Pipeline

An end-to-end Data Engineering project that automates the extraction, transformation, and loading (ETL) of Australian weather data into a PostgreSQL Data Warehouse using **Apache Airflow**.

## Project Overview
This project simulates a daily batch-processing pipeline. It ingests historical weather data, performs data quality checks and transformations, and organizes the output into a **Star Schema** for optimized downstream analytics and Business Intelligence (BI) usage.

## Architecture & Technologies
* **Orchestration:** Apache Airflow
* **Data Processing:** Python (Pandas)
* **Data Warehouse:** PostgreSQL
* **Infrastructure:** Docker & Docker Compose
* **Version Control:** Git & GitHub

## Key Features & Best Practices

### 1. Data Modeling (Star Schema)
Transformed raw flat files into a relational Star Schema to reduce data redundancy and optimize query performance:
* **Fact Table:** `fact_weather` (Stores measures like Min/Max Temp, Rainfall, Humidity)
* **Dimension Tables:** `dim_location`, `dim_date`

### 2. Data Quality & Optimization
* **Data Imputation:** Handled missing humidity values using mean imputation to prevent data loss.
* **Column Pruning:** Kept only essential columns to optimize processing.
* **Storage Optimization:** Converted raw `.csv` into `.parquet` format with partitioned storage (`year=.../month=...`) for faster I/O operations.

### 3. Reliability (Idempotency)
Implemented `DELETE BEFORE INSERT` logic within the pipeline. This guarantees **Idempotency**—ensuring that rerunning the pipeline for the same `execution_date` will never result in duplicate data in the database.

### 4. Production-Grade Security
* **Environment Variables:** Used `.env` and `.gitignore` to securely hide database credentials.
* **Credential Management:** Leveraged Airflow's Connection UI with **Fernet Key Encryption** via `PostgresHook`, completely avoiding hardcoded passwords in the Python scripts.



## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose installed
* Git

### Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/PariithySwu/weather-etl-airflow.git](https://github.com/PariithySwu/weather-etl-airflow.git)
   cd weather-etl-airflow

2. **Set up Environment Variables:**
   ```bash
   # Create a .env file in the root directory and add your secure credentials (this file is git-ignored):
   cat <<EOF > .env
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=your_secure_password
   AIRFLOW__CORE__FERNET_KEY=your_generated_fernet_key
   EOF

3. **Start the Infrastructure:**
   `docker compose up -d`

4. **Configure Airflow Connection:**
* Access the Airflow UI at http://localhost:8080 (Default login: airflow/airflow).
* Navigate to Admin > Connections and add a new Postgres connection.
* Set Connection Id to postgres_weather_db and enter your database credentials.

5. **Run the Pipeline:**
Turn on the toggle for the weather_etl_dag in the Airflow UI to start the automated schedule or trigger it manually.



## Project Structure

```text
├── dags/
│   └── weather_etl_dag.py     # Airflow DAG definition and Python ETL logic
├── data/
│   ├── staging/               # Partitioned Parquet files (Year/Month)
│   └── weatherAUS.csv         # Raw source data
├── config/                    # Airflow configurations
├── logs/                      # Airflow task logs
├── plugins/                   # Custom Airflow plugins
├── .env                       # Environment variables (Ignored by Git)
├── .gitignore                 # Files to exclude from version control
├── docker-compose.yaml        # Docker infrastructure setup
└── requirements.txt           # Python dependencies

---

Pariyakorn Charumit 66102010174
