# eCommerceAnalyticsOps Data Pipeline

## Key Technologies
Apache Airflow - Docker - PostgreSQL - Python - SQL - Power BI

## Description
This project implements an end-to-end analytics pipeline for a large, multi-table eCommerce dataset representing a single business.

The pipeline automates ingestion, transformation, and loading of data into a relational database, where SQL-based analytics views are built for downstream reporting and visualization. The focus is on transforming raw operational data into reliable, analytics-ready structures that support KPI reporting and business insights.

## Dataset
theLook eCommerce Dataset (fictional business data)
Multi-table structure including:
Orders, Order items, Products, Users (customers)

## ETL Process
1. Airflow orchestrates repeatable, idempotent ETL tasks
2. PostgreSQL serves as the analytical warehouse
3. Raw data is preserved separately from analytics-ready views
4. Docker ensures full reproducibility across environments

## KPIs
Daily & Monthly Revenue
Order Count & Item Volume
Average Order Value (AOV)
Revenue by Product Category
Customer Retention

## Future Improvements
TBD
