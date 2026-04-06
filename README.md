# Travel Booking — SCD Type 2 ETL Pipeline

A production-grade, parameterized ETL pipeline for travel booking data built on **Databricks** and **Delta Lake**. The pipeline implements **SCD Type 2** dimension handling for customers, ensures data quality at every stage, builds incremental fact tables, and generates business reports for revenue and customer insights.

## 🔧 Tech Stack
- Databricks (PySpark + SQL)
- Delta Lake (SCD2 MERGE, Z-Order optimization)
- Azure Data Lake Storage Gen2 (ADLS)
- Python
- Databricks Workflows (Job orchestration)

## 🔄 Pipeline Flow
Source Data (ADLS)
↓
Input Validation
↓
Bronze Ingestion (Bookings + Customers)
↓
Data Quality Checks
↓
SCD2 Merge → Customer Dimension Table
Incremental Build → Booking Fact Table
↓
Delta Optimization (Z-Order)
↓
Stats Analysis + SQL Reports

## 📁 Repository Structure
Travel_Booking_SCD2_Project/
├── notebooks/
│   ├── validate_inputs.ipynb
│   ├── Ingest_bookings_bronze.ipynb
│   ├── Ingest_customers_bronze.ipynb
│   ├── dq_customers.ipynb
│   ├── dq_bookings.ipynb
│   ├── customer_dim_scd2.ipynb
│   ├── booking_fact_build.ipynb
│   ├── optimize_zorder.ipynb
│   └── analyze_stats.ipynb
├── queries/
│   ├── travel_booking_init.ipynb
│   ├── data_quality.ipynb
│   ├── daily_revenue.ipynb
│   ├── customer_360.ipynb
│   └── log_completion_flow.ipynb

## 📓 Workflow 1 — Parameterized ETL & Data Quality Notebooks

| Notebook | Description |
|---|---|
| `validate_inputs` | Input validation for arrival_date and source data parameters |
| `Ingest_bookings_bronze` | Ingests raw bookings data into Bronze Delta table |
| `Ingest_customers_bronze` | Ingests raw customer data into Bronze Delta table |
| `dq_customers` | Data quality checks for customers dataset |
| `dq_bookings` | Data quality checks for bookings dataset |
| `customer_dim_scd2` | SCD Type 2 merge logic for customer dimension table |
| `booking_fact_build` | Incremental fact table build for bookings |
| `optimize_zorder` | Optimizes Delta tables using Z-Order for query performance |
| `analyze_stats` | Generates Delta table statistics for monitoring |

## 📊 Workflow 2 — SQL Reporting (triggered after Workflow 1)

| Query | Description |
|---|---|
| `travel_booking_init` | Initializes tables required for reporting |
| `data_quality` | Generates data quality summary reports |
| `daily_revenue` | Daily revenue aggregation across bookings |
| `customer_360` | Creates a Customer 360 view with full booking history |
| `log_completion_flow` | Logs job completion status and pipeline metrics |

## 📌 Key Concepts Covered
- Parameterized notebooks for reusable and flexible pipeline execution
- Bronze layer ingestion for bookings and customer data from ADLS
- Data quality checks at both bookings and customers level
- SCD Type 2 implementation using Delta Lake MERGE for customer dimension
- Incremental fact table build for daily bookings
- Delta table optimization using Z-Order for faster query performance
- SQL-based reporting for revenue analytics and Customer 360 view
- Pipeline job completion logging

## 🚀 How to Run

**Workflow 1 — ETL & Data Quality:**
1. Configure parameters: `arrival_date` and ADLS source path.
2. Run `validate_inputs` to validate input parameters.
3. Run `Ingest_bookings_bronze` and `Ingest_customers_bronze` to load raw data into Bronze tables.
4. Run `dq_customers` and `dq_bookings` for data quality validation.
5. Run `customer_dim_scd2` to apply SCD2 merge on customer dimension.
6. Run `booking_fact_build` to incrementally build the bookings fact table.
7. Run `optimize_zorder` to optimize Delta tables.
8. Run `analyze_stats` to generate table statistics.

**Workflow 2 — SQL Reports (after Workflow 1 completes):**
1. Run `travel_booking_init` to initialize reporting tables.
2. Run `data_quality`, `daily_revenue`, and `customer_360` for reports.
3. Run `log_completion_flow` to log pipeline completion status.

## 🔮 Next Steps
- Automate workflow scheduling via Databricks Jobs
- Add alerts for data quality failures

## 👤 Author
Shashank Shukla — [GitHub](https://github.com/shukla2015)
