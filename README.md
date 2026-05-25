# 🛒 SmartShop Analytics Platform
### Real-time E-commerce Data Pipeline | Brazilian Market Intelligence

![Pipeline](https://img.shields.io/badge/Pipeline-End--to--End-brightgreen)
![Kafka](https://img.shields.io/badge/Kafka-2.3.1-black)
![PySpark](https://img.shields.io/badge/PySpark-3.5.3-orange)
![dbt](https://img.shields.io/badge/dbt-1.11.8-red)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-blue)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)

---

## 📊 Live Dashboard
👉 **[View SmartShop Analytics Dashboard](https://dbc-d50823ca-98cf.cloud.databricks.com/dashboardsv3/01f157c9377d1441aacd65c1a22912d8/published?o=7474652948250215)**

![Dashboard Preview](https://img.shields.io/badge/Dashboard-Live-brightgreen)

**Key Metrics:**
- 💰 Total Revenue: **$15.42M**
- 📦 Total Orders: **96,480+**
- 💵 Avg Order Value: **$153.66**
- 🏆 Top State: **São Paulo (SP)**

---

## 🏗️ Architecture

```
[Olist CSV Data]
      ↓
[Python Producer]     → Simulates real-time order streaming
      ↓
[Apache Kafka]        → Event streaming broker (Docker)
      ↓
[PySpark Streaming]   → Real-time data processing & enrichment
      ↓
[Delta Lake]          → Local data lake storage (Parquet)
      ↓
[Databricks]          → Cloud analytics platform
      ↓
[dbt]                 → Data transformation & modeling
      ↓
[Dashboard]           → Business intelligence & visualization
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Apache Kafka | Real-time event streaming |
| **Processing** | PySpark 3.5.3 | Stream processing & transformation |
| **Storage** | Delta Lake | Data lake with ACID transactions |
| **Cloud Platform** | Databricks | Cloud analytics & SQL queries |
| **Transformation** | dbt 1.11.8 | Data modeling & aggregation |
| **Containerization** | Docker | Local infrastructure management |
| **Language** | Python 3.11.9 | Pipeline development |
| **Visualization** | Databricks Dashboard | Business intelligence |

---

## 📁 Project Structure

```
Smartshop Analytical Platform/
│
├── data/                              # Olist e-commerce dataset (CSV)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_products_dataset.csv
│   └── product_category_name_translation.csv
│
├── producer/
│   └── order_producer.py              # Kafka producer — streams orders
│
├── consumer/
│   └── spark_consumer.py              # PySpark streaming consumer
│
├── delta_lake/                        # Local Delta Lake storage
│   ├── orders/                        # Processed order parquet files
│   └── checkpoints/                   # Spark streaming checkpoints
│
├── smartshop_dbt/                     # dbt project
│   └── models/
│       └── smartshop/
│           ├── daily_revenue.sql      # Daily revenue aggregation
│           ├── top_products.sql       # Top products by revenue
│           ├── order_status_summary.sql # Order fulfillment metrics
│           └── customer_analytics.sql # Customer segmentation
│
├── docker-compose.yml                 # Kafka + Zookeeper setup
├── requirements.txt                   # Python dependencies
└── README.md
```

---

## 📦 Dataset

This project uses the **[Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** from Kaggle.

| File | Records | Description |
|---|---|---|
| olist_orders_dataset.csv | 99,441 | Main orders table |
| olist_order_items_dataset.csv | 112,650 | Order line items |
| olist_order_payments_dataset.csv | 103,886 | Payment transactions |
| olist_customers_dataset.csv | 99,441 | Customer details |
| olist_products_dataset.csv | 32,951 | Product catalog |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.11+
- Docker Desktop
- Java 17 (for PySpark)
- Databricks account (free edition)
- Kaggle account (for dataset)

### Installation

**1 — Clone the repository:**
```bash
git clone https://github.com/yourusername/smartshop-analytics-platform.git
cd smartshop-analytics-platform
```

**2 — Create and activate virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # Mac/Linux
```

**3 — Install dependencies:**
```bash
pip install -r requirements.txt
```

**4 — Download the dataset:**
Download from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place CSV files in the `data/` folder.

**5 — Start Kafka:**
```bash
docker-compose up -d
```

**6 — Stream orders into Kafka:**
```bash
python3 producer/order_producer.py
```

**7 — Start Spark consumer:**
```bash
python3 consumer/spark_consumer.py
```

**8 — Run dbt models:**
```bash
cd smartshop_dbt
dbt run
```

---

## 📊 dbt Models

| Model | Description | Source Tables |
|---|---|---|
| `daily_revenue` | Revenue and orders aggregated by day | orders, payments |
| `top_products` | Top 20 product categories by revenue | items, products, orders |
| `order_status_summary` | Order fulfillment rate analysis | orders |
| `customer_analytics` | Revenue and orders by Brazilian state | customers, orders, payments |

---

## 📈 Key Business Insights

From the SmartShop Analytics dashboard:

- **$15.42M** total revenue generated across 2016-2018
- **97.02%** successful order delivery rate
- **Health & Beauty** is the top revenue category at **$1.23M**
- **São Paulo** dominates with **$5.77M** in revenue (37% of total)
- Revenue peaked in **November 2017** reaching **$175K+ in a single day**
- Average order value of **$153.66** across all delivered orders

---

## 🔧 Configuration

### Kafka Setup
Kafka runs on `localhost:9092` via Docker. Topics created automatically:
- `orders` — real-time order events

### dbt Configuration
Configure `~/.dbt/profiles.yml` with your Databricks credentials:
```yaml
smartshop_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      host: your-databricks-host
      http_path: your-http-path
      token: your-access-token
      schema: default
      catalog: workspace
```

---

## 🎯 Skills Demonstrated

- **Stream Processing** — Real-time data ingestion with Kafka and PySpark
- **Data Lake** — Delta Lake with ACID transactions and time travel
- **Data Modeling** — dbt transformations and business logic
- **Cloud Analytics** — Databricks SQL and dashboard creation
- **Data Engineering** — End-to-end pipeline design and implementation
- **DevOps** — Docker containerization and environment management

---

## 📝 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgements

- [Olist](https://olist.com/) for the Brazilian e-commerce dataset
- [Apache Kafka](https://kafka.apache.org/) for the streaming platform
- [Delta Lake](https://delta.io/) for reliable data lake storage
- [dbt Labs](https://www.getdbt.com/) for the transformation framework
- [Databricks](https://databricks.com/) for the analytics platform
