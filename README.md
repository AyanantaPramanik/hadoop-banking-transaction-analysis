# 🏦 Hadoop Banking Transaction Analyser

A scalable data engineering and analytics project built using **Python**, **Hadoop (HDFS)**, **PySpark**, and **Power BI**, simulating large-scale banking transactions.

---

## ⚙️ Project Summary

This project demonstrates a complete pipeline:
- 🔧 **Synthetic Data Generation** via Python
- 💾 **HDFS Integration** to store large files
- 🔥 **Data Processing with PySpark** to perform aggregations & fraud checks
- 📊 **Power BI Dashboard** for insights on transaction types, failures, merchant trends

---

## 🗂️ Key Files

| File | Description |
|------|-------------|
| `transaction_generator.py` | Generates realistic transaction data |
| `transactions.json`        | Output JSON file pushed to HDFS |
| `banking_analysis.py`      | PySpark script for ETL and analysis |
| `analysis_results/*.csv`   | Aggregated outputs for visualization |
| `output_summary.csv`       | Cleaned summary dataset for reporting |

---

## 🛠️ Tools & Tech

- Python 3.x  
- Hadoop HDFS (local/WSL)  
- PySpark  
- Power BI  
- Git & GitHub

---

## 🚀 Output Metrics

- Top merchants by revenue  
- Failure rates by city and merchant  
- Average transaction amount per user  
- Success vs Failed transaction breakdown

---

## 📊 Dashboard (Optional)

Visual insights built in **Power BI** using exported `.csv` files.

---

## 🔗 Connect

**Author**: [Ayananta Pramanik](https://github.com/AyanantaPramanik)  
**Email**: ayananta@gmail.com  
**LinkedIn**: [Profile](https://www.linkedin.com/in/ayananta-pramanik/)

---

> 🎯 *Built for data scale. Designed for impact.*
