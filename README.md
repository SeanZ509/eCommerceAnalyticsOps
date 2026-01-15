# E-Commerce Analytics Pipeline & KPI Reporting System

## Key Technologies
Apache Airflow - Docker - PostgreSQL - Python - SQL - Power BI

## Executive Summary
This project demonstrates how raw eCommerce operational data can be transformed into reliable, analytics-ready datasets that support recurring KPI reporting and business decision-making.

I designed and implemented an end-to-end analytics pipeline that automates data ingestion, transformation, and validation using SQL and Python, and delivers standardized KPIs through BI dashboards. The pipeline emphasizes data quality, reproducibility, and separation of raw vs analytical data — patterns commonly used in production analytics environments.

## Business Context
Leadership and operations teams need consistent, trustworthy metrics to monitor revenue performance, customer behavior, and product trends. Manual reporting and inconsistent transformations create risk and slow decision-making.

Simulates a real eCommerce business environment where analysts must:
- Ingest raw transactional data
- Apply repeatable transformations
- Define consistent KPI logic
- Deliver insights via dashboards

## ETL Process
1. Airflow orchestrates repeatable, idempotent ETL tasks
2. PostgreSQL serves as the analytical warehouse
3. Raw data is preserved separately from analytics-ready views
4. Docker ensures full reproducibility across environments

## KPIs
Daily & Monthly Revenue,
Order Count & Item Volume,
Average Order Value (AOV),
Revenue by Product Category,
Customer Retention

## Future Improvements
TBD
