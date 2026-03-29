End-to-End Advanced Analytics Pipeline

Segmentation • Forecasting • Association Rules • Next Best Action

Overview

This project is a production-grade end-to-end analytics pipeline built on the Brazilian E-Commerce dataset (Olist). It transforms raw transactional data into actionable business insights and intelligent decision systems.

The pipeline integrates data engineering, advanced analytics, and business intelligence to answer key business questions:

Who are our most valuable customers?
What will future sales look like?
Which products are frequently bought together?
What is the next best action to increase revenue?

Problem Statement

Raw transactional data often lacks:

Analytical data models
Customer segmentation
Forecasting capabilities
Cross-sell intelligence
Decision-driven outputs

This leads to limited visibility into customer behavior, sales performance, and growth opportunities.

Solution

This project delivers a fully integrated analytics ecosystem:

End-to-end ELT pipeline
Scalable data warehouse
Advanced analytical models
Interactive BI dashboards
Marketing-ready outputs (Next Best Action)

Source (Olist Dataset)
   ↓
Neon PostgreSQL (OLTP)
   ↓
Airbyte (ELT Ingestion)
   ↓
BigQuery (Data Warehouse)
   ↓
dbt (Transformation & Modeling)
   ↓
Power BI (Visualization)

Data Model

Galaxy Schema Design

Dimension Tables
dim_customer
dim_product
dim_seller
dim_date
dim_payment
Fact Tables
fact_sales
fact_order_items
fact_payment
fact_reviews
Aggregates
agg_sales_trends
agg_cohort_retention
agg_product_quality_residual
Marketing Layer
mkt_recs_product
mkt_nba_category (Next Best Action)

Analytical Methods
1. Customer Segmentation (RFM)
Recency, Frequency, Monetary scoring
Segment groups: Champions, Loyal, Hibernating, etc.
2. Customer Lifetime Value (CLV)
Predictive CLV modeling
Customer tiering: Platinum, Gold, Silver, Bronze
3. Cohort Retention Analysis
Monthly cohort tracking
Retention decay analysis
4. Market Basket Analysis
Association rule mining
Cross-sell recommendations
Foundation for Next Best Action
5. Sales Forecasting and Momentum
Moving averages (7-day, 30-day)
Trend classification (upward or downward)
6. Product Quality Scoring (OLS Regression)
Residual analysis on delivery vs review score
Identifies over- and under-performing products
Key Insights
R$ 6.27M GMV tracked
95% of customers are one-time buyers
Low repeat rate (2.78%) indicates retention opportunity
Top 5 categories contribute more than 40% of revenue
6.16% late delivery rate impacts customer satisfaction
More than 2,000 cross-sell opportunities identified

Dashboards
1. Executive Summary
GMV trends (MoM and QoQ)
Regional performance
Category contribution
2. Sales Performance
Sales momentum (EMA)
GMV heatmaps
Pareto category analysis
Product affinity matrix
3. Customer Insights
RFM segmentation
CLV prediction
Cohort retention
Product quality residuals
Tech Stack

Layer	Tools
Data Source	Olist Dataset (Kaggle)
OLTP	Neon PostgreSQL
Ingestion	Airbyte
Warehouse	Google BigQuery
Transformation	dbt Core
Visualization	Power BI
Programming	SQL, Python

End-to-End Workflow
Ingest raw data from PostgreSQL using Airbyte
Store data in BigQuery staging layer
Transform data using dbt (modular SQL models)
Build marts for analytics and marketing
Apply advanced analytics techniques
Deliver insights via Power BI dashboards
Business Value
Enables data-driven decision making
Improves customer targeting and retention
Generates Next Best Action recommendations
Identifies operational inefficiencies
Unlocks revenue growth opportunities
Future Improvements
Machine learning-based demand forecasting
Real-time streaming pipeline
Personalized recommendation engine
A/B testing framework for Next Best Action strategies

Author

Abdun Fattah Yolandanu
Data Analyst Portfolio • 2026
