#  Motor Sales & Service Analytics (Azure Data Engineering Project)

##  Overview
This project demonstrates an **end-to-end Azure Data Engineering pipeline** for a motor company (Hyundai-like scenario), focusing on **sales performance, service operations, and customer analytics**.

The solution integrates multiple data sources and processes them using a **Medallion Architecture (Bronze → Silver → Gold)** to generate business insights.

---

##  Technologies Used
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks (PySpark)
- Delta Lake
- SQL Server
- Power BI

---

##  Data Sources
| Source System | Data |
|--------------|------|
| SQL Server | Customer, Vehicle, Dealership |
| Blob Storage | Sales, Service, Spare Parts |
| REST API | Customer Loyalty Data |

---

##  Architecture
SQL Server / Blob / API
↓
Azure Data Factory
↓
ADLS (Raw Layer)
↓
Databricks (Bronze → Silver → Gold)
↓
Power BI Dashboard
