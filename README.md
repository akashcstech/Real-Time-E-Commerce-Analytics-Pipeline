# Real-Time E-Commerce Analytics Pipeline

A simplified, modular, and production-style real-time analytics pipeline for e-commerce event data.
It includes data generation, processing, modeling, and orchestration—fully containerized using Docker.

---

## 🚀 Project Overview

This project demonstrates how real-time e-commerce events (clicks, orders, sessions, etc.) flow through a modern analytics pipeline through batch job and streaming job:

1. **Data Simulator** generates fake streaming events
2. **Spark** processes and transforms these events
3. **dbt** models analytics tables
4. **Airflow** orchestrates the entire workflow
5. Everything runs inside **Docker**

This architecture mirrors real-world systems used by modern data engineering teams.

---

## 🏗️ Architecture

**Flow:**

```
Data Simulator → Spark Processing → Raw/Processed Storage → dbt Models → Analytics Tables
                       ↑
                    Orchestrated by Airflow
```

---

## 📁 Directory Structure

```
Real-Time-E-Commerce-Analytics-Pipeline/
│
├── airflow/              # DAGs, Airflow config
├── data-simulator/       # Event generator
├── dbt/
│   └── ecommerce_dbt/    # dbt models
├── spark/                # Spark jobs (batch/streaming)
│
├── docker-compose.yaml   # End-to-end stack
├── dockerfile.spark      # Spark image build
├── dockerfile.airflow    # Airflow image build
└── README.md
```

---

## ⚙️ Prerequisites

Before running the project, ensure you have:

* **Docker**
* **Docker Compose**
* **Python 3.9+** (if running simulator manually)
* **dbt** (optional for local runs — Airflow can run it)

---

## ▶️ Getting Started

### 1️⃣ Clone the repository

```bash
git clone https://github.com/akashcstech/Real-Time-E-Commerce-Analytics-Pipeline
cd Real-Time-E-Commerce-Analytics-Pipeline
```

### 2️⃣ Start all services

```bash
docker-compose up --build
```

This launches:

* Spark Master & Worker
* Airflow Scheduler & Webserver
* Data Simulator
* dbt environment

### 3️⃣ Access UIs

| Service        | URL                                            |
| -------------- | ---------------------------------------------- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) |
| **Spark UI**   | [http://localhost:8081](http://localhost:8081) |

---

## 🔄 Pipeline Workflow

### 1. Data Simulator

Generates JSON events (clicks, orders, sessions, etc.).

### 2. Spark

Ingests events (stream/batch), transforms them, and writes processed outputs.

### 3. dbt

Builds analytics-ready models (fact tables, dimensions, aggregates).

### 4. Airflow

Orchestrates:

* Data generation
* Spark processing
* dbt runs
* Quality checks

---

## 📊 Example Use Cases

* Real-time order analytics
* User behavior events
* Product funnel insights
* Aggregated KPIs
* Customer metrics

---

## 🛠️ Configuration

* Edit environment variables inside `docker-compose.yaml`
* Update dbt connection in `dbt/profiles.yml`
* Modify Spark job configs inside `spark/` folder

---

## 🧪 Testing the Pipeline

You can run each component independently:

**Run simulator only**

```bash
python data-simulator/producer.py
```

**Run dbt**

```bash
cd dbt/ecommerce_dbt
dbt run
```

**Run Spark jobs**

```bash
spark-submit spark/job.py
```

---

## 🐛 Troubleshooting

| Issue                     | Fix                                       |
| ------------------------- | ----------------------------------------- |
| Spark can't read messages | Check simulator output path / Kafka topic |
| Airflow DAG failing       | Open Airflow UI → check task logs         |
| dbt error                 | Ensure `profiles.yml` is correct          |
| Docker errors             | Run `docker system prune` and rebuild     |

---

## 📌 Future Enhancements

* Option to use Kafka instead of local simulator
* Add Snowflake or BigQuery as warehouse
* Add dashboards (Metabase / Looker / Power BI)
* Add data quality monitoring

---

## 🤝 Contributing

Pull requests are welcome!
Please open an issue for major changes.

---

## 📄 License

This project is currently unlicensed.
Add a LICENSE file if you want others to use it legally.

---
