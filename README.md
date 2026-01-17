# Revenue Leakage Analysis Project

## Project Overview
A comprehensive SQL-based data pipeline that detects and analyzes revenue leakage in e-commerce transactions. This project simulates real-world data validation, cleaning, and business intelligence processes to identify financial losses from pricing errors, discount mismatches, and data inconsistencies.

### Key Business Questions Answered:
- **How much revenue are we losing due to pricing/discount errors?**
- **Which products have the highest revenue leakage?**
- **Are customers abusing discount systems?**
- **What's our actual profitability per product after costs?**
- **Which customers pose the highest financial risk?**


---

## Architecture: Bronze → Silver → Gold Layers

### **BRONZE LAYER (Raw Data)**
- **Purpose**: Initial data ingestion and basic validation
- **Activities**:
  - Bulk CSV loading into MySQL
  - Duplicate detection
  - Referential integrity checks
  - Null and boundary value validation

### **SILVER LAYER (Cleaned Data)**
- **Purpose**: Data quality enforcement and transformation
- **Activities**:
  - Price calculation validation
  - Discount application verification
  - Margin consistency checks
  - Customer data cleaning

### **GOLD LAYER (Business Insights)**
- **Purpose**: Generate actionable business intelligence
- **Activities**:
  - Revenue leakage quantification
  - Customer risk profiling
  - Product profitability analysis
  - Channel performance evaluation

