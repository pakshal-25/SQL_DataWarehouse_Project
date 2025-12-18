# SQL_DataWarehouse_Project

Welcome to the SQL Data Warehouse Project 🚀  
This repository demonstrates a complete end-to-end data engineering solution using SQL Server and Medallion architecture. It is designed as a portfolio project to showcase real-world skills in data ingestion, transformation, modeling, and analytics.

Data Architecture

The project follows a modern Medallion Architecture:

1. Bronze Layer  
   Raw data ingestion directly from source CSV files.

2. Silver Layer  
   Data cleansing, transformation, and normalization.

3. Gold Layer  
   Business-ready star schema tables optimized for analytics and reporting.

This layered structure improves data quality, scalability, and maintainability.


Project Overview

This project includes:

- Designing a modern SQL data warehouse  
- Building ETL pipelines using SQL Server  
- Cleaning and standardizing raw ERP and CRM data  
- Creating dimension and fact tables  
- Applying star schema data modeling  
- Performing foreign key integrity checks  
- Running analytical queries on curated data  

By completing this project, you demonstrate strong skills in:

- SQL development  
- Data engineering and ETL  
- Data modeling  
- Data quality handling  
- Analytical reporting
  

🚀 Project Requirements
Building the Data Warehouse (Data Engineering)
Objective
Develop a modern data warehouse using Azure Data Studio to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications
Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
Data Quality: Cleanse and resolve data quality issues prior to analysis.
Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
Scope: Focus on the latest dataset only; historization of data is not required.
Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.



data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
