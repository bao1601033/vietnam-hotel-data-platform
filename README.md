# Vietnam Hotel Data Platform

Cloud-native data engineering platform for collecting, processing, and analyzing hotel data across Vietnam. The project implements an automated **Bronze–Silver–Gold data lakehouse** on AWS and provides a **SQL-RAG natural language interface** for analytical hotel search.

## Overview

Hotel information is fragmented across online booking platforms and often comes in inconsistent structures and formats. This project centralizes hotel data into a structured analytical platform that supports both traditional SQL analytics and natural-language exploration.

The platform automates the data lifecycle from web data collection to cloud storage, transformation, analytical modeling, and AI-powered querying.

## Architecture
<img width="920" height="443" alt="Screenshot 2026-07-09 at 11 28 22 PM" src="https://github.com/user-attachments/assets/8f487a80-6569-4525-9a24-8fe5f795c8eb" />

## Data Pipeline

### 1. Data Collection

Hotel data is collected from web sources using Python-based scraping scripts.

The ingestion process extracts relevant hotel attributes such as:

* Hotel name
* Location
* Star rating
* Review score
* Price
* Address
* Hotel metadata

### 2. Bronze Layer

Raw scraped data is stored in **AWS S3** using Parquet format.

The Bronze layer preserves the original ingested data and provides a historical storage boundary for downstream processing.

### 3. Silver Layer

The Silver layer transforms raw records into clean and standardized datasets.

Processing includes:

* Schema normalization
* Data type standardization
* Duplicate removal
* Missing-value handling
* Data cleaning
* Structural transformation

AWS Glue and SQL are used for transformation and data cataloging.

### 4. Gold Layer

The Gold layer contains business-oriented datasets optimized for analytical queries.

The data is structured around common hotel analysis dimensions such as:

* Hotel
* Location
* Rating
* Pricing
* Reviews

Amazon Athena provides serverless SQL access to the analytical datasets stored in S3.

### 5. AI / SQL-RAG Layer

The platform provides a natural-language interface using **Claude API**.

Instead of relying on vector similarity for structured hotel attributes, user queries are interpreted and translated into SQL statements that can be executed against the Gold layer.

Example:

> "Find 5-star hotels in Da Nang near the beach with a rating above 4.5."

The query is interpreted into structured SQL conditions such as:

```sql
WHERE star_rating = 5
  AND city = 'Da Nang'
  AND rating > 4.5
```

This approach provides deterministic filtering for structured attributes such as price, rating, location, and hotel category.

## Orchestration

**Apache Airflow** is used to orchestrate the end-to-end data workflow.

The pipeline can be scheduled to:

1. Collect new hotel data
2. Store raw data in S3
3. Trigger downstream transformations
4. Update analytical datasets

This separates data collection, transformation, and analytical processing into manageable pipeline stages.

## Technology Stack

### Data Engineering

* **Python** — scraping, data processing, and pipeline logic
* **Apache Airflow** — workflow orchestration and scheduling
* **Pandas** — initial data processing and structural transformation
* **SQL** — data transformation and analytical querying

### AWS

* **Amazon S3** — cloud data lake storage
* **AWS Glue** — serverless ETL and data cataloging
* **Amazon Athena** — serverless SQL analytics

### Storage

* **Apache Parquet** — columnar storage format optimized for analytical workloads

### AI

* **Claude API** — natural-language understanding and SQL generation
* **SQL-RAG** — retrieval approach connecting natural-language queries with structured analytical data

## Key Engineering Decisions

### Medallion Architecture

The Bronze–Silver–Gold architecture separates raw, cleaned, and business-ready datasets.

This provides:

* Clear data processing boundaries
* Easier debugging
* Data lineage between processing stages
* Historical data preservation
* Ability to reprocess downstream layers

### Serverless AWS Architecture

S3, Glue, and Athena reduce infrastructure management requirements while providing scalable storage and serverless analytical processing.

### SQL-RAG Instead of Pure Vector Search

Hotel data contains many structured attributes that require exact filtering.

For example:

* Price < 2,000,000 VND
* Rating > 4.5
* 5-star hotels
* Hotels within a specific location

SQL provides deterministic filtering for these requirements, making it more suitable than relying solely on semantic vector similarity.

## Example Query

### Natural Language

```text
Find 5-star hotels in Da Nang near the beach
with a rating above 4.5 and price below 2 million VND.
```

### SQL

```sql
SELECT
    hotel_name,
    star_rating,
    rating,
    price,
    location
FROM gold_hotels
WHERE city = 'Da Nang'
  AND star_rating = 5
  AND rating > 4.5
  AND price < 2000000;
```

The SQL-RAG layer bridges the natural-language request and the structured analytical dataset.

## Project Structure

```text
vietnam-hotel-data-platform/
│
├── airflow/
│   └── dags/
│       └── ...
│
├── data/
│   └── ...
│
├── scraper_final.py
├── start_airflow.sh
├── stop_airflow.sh
├── README.md
└── .gitignore
```

## Key Deliverables

* Automated hotel data collection pipeline
* AWS S3-based Medallion data lake
* Airflow workflow orchestration
* Serverless ETL and data cataloging with AWS Glue
* Athena-based analytical querying
* Parquet-based analytical storage
* Natural-language-to-SQL search using Claude API

## Future Improvements

Potential extensions include:

* Data quality testing with Great Expectations
* Automated data quality monitoring and alerting
* CI/CD for pipeline validation and deployment
* Incremental and near-real-time ingestion using Kafka or AWS Kinesis
* Expansion to additional tourism datasets such as flights and attractions

## Author

**Phan Chi Bao**

Personal Data Engineering project focused on:

* Cloud-native data pipelines
* Data lakehouse architecture
* Workflow orchestration
* Analytical data modeling
* Serverless AWS analytics
* Natural-language-to-SQL applications
