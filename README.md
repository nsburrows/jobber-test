# Jobber Data Pipeline

This project implements a PySpark-based ETL (Extract, Transform, Load) pipeline to process customer and sales data, enhance it with derived metrics, and store the cleaned dataset in Delta Lake format for analytics and reporting.

---

## 📂 Project Structure

```
JobberDataPipeline/
├── data/
│   ├── customer_data.parquet
│   ├── sales_data.csv
│   └── full_sales_data.delta/
├── JobberDataPipeline.py
├── README.md
└── requirements.txt
```

---

## 🚀 Features

✅ Reads **customer data** (Parquet) and **sales data** (CSV).\
✅ Cleans missing and duplicate records.\
✅ Infers data types and fills missing age values with average age.\
✅ Enhances sales data by adding `total_price` (quantity \* price, rounded to 2 decimals).\
✅ Joins customer and sales datasets (left join on `customer_id`).\
✅ Saves the final dataset as a **Delta Lake table**, partitioned by `year` and `month`.\
✅ Tracks total processing time for the pipeline.

---

## 🖥 Prerequisites

- **Java Development Kit (JDK)** 17\
  Download: [Adoptium Temurin JDK 17](https://adoptium.net/en-GB/temurin/releases/?version=17)
- **Python** 3.10 or higher (tested with Python 3.13)
- **Apache Spark** 3.5.x
- **Delta Lake** 3.1.0 (for Spark 3.x)

---

## 📦 Installation

1. **Clone the repository**

      ```bash
   git clone https://github.com/nsburrows/jobber-test.git
   cd jobber-test
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate   # Windows
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Ensure Spark and Delta JARs are available**

   - Delta Lake JARs are retrieved via `spark.jars.packages` in the script.

---

## ⚡ Running the Pipeline

```bash
python JobberDataPipeline.py
```

The script will:

- Load `customer_data.parquet` and `sales_data.csv` from the `./data/` directory.
- Perform ETL transformations.
- Save the resulting Delta Lake table to `./data/full_sales_data.delta/`.

Expected console output:

```
Starting the Jobber Data Pipeline...
Number of customers: 99457
Number of sales: 99457
Number of records in Delta table: 99457
Finished processing the Jobber Data Pipeline in 30.51 seconds.
```

---

## 🗂 Delta Table Partitioning

The Delta table is partitioned by:

- **Year** (`year`)
- **Month** (`month`)

This improves query performance for time-based analytics.

Example partition path:

```
./data/full_sales_data.delta/year=2025/month=07/
```

---

## ⚙ Configuration Options

- **Delta Format Version**: 3.1.0
- **Partitions**: `year`, `month`
- **Shuffle Partitions**: Default to 8 (can be configured in `spark.sql.shuffle.partitions`)

---

## 👨‍💻 Author

**Nigel Burrows**\
Senior Data Engineer