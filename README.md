**Overview**

This project establishes a comprehensive data pipeline designed to collect, process, and analyze hotel data across Vietnam. The primary goal is to centralize fragmented lodging information scattered across the internet into a single, scalable data lakehouse. By combining modern data engineering workflows with a SQL-RAG conversational interface, the system enables end-users to intuitively query and explore hotel metrics using natural language.

**Business Problem**

As Vietnam's tourism industry experiences rapid growth, a massive volume of accommodation data is continuously generated across various online booking platforms. However, leveraging this data presents several critical technical bottlenecks. First, the data is highly fragmented, remaining scattered across isolated external web channels under diverse formats. Second, there is a lack of a centralized repository required to perform comprehensive, market-wide analysis. Finally, traditional search interfaces are significantly limited when users attempt to filter information by multiple complex criteria simultaneously, such as combining strict budget limits, hyper-local geolocations, and dynamic historical review scores. This project resolves these challenges by automating the data ingestion workflow and deploying an intelligent analytical layer.

**Solution Overview**

The system processes data sequentially from initial ingestion to downstream consumption. To begin, raw data from external web sources is systematically gathered via automated tools within the Data Collection Pipeline. This raw dataset is then loaded directly into the Bronze Layer for immutable historical storage. Following this, data cleaning, duplicate removal, and schema normalization operations take place to transition the records into a trusted Silver Layer. From there, the data is transformed and organized into optimized business tables within the Gold Layer to support advanced analytical workloads. Finally, the AI Conversational Search interface utilizes a SQL-RAG architecture to translate unstructured text queries from users into precise SQL commands, which execute directly against the Gold Layer tables.

**Technology Stack**

**1. Data Engineering & Storage**
Python: Core runtime environment for scripting, scraping, and transformations.

Apache Airflow: Workflow orchestration to schedule and monitor the end-to-end ETL processes.

AWS S3: Centralized Cloud Data Lake storing the Parquet-formatted Medallion layers.

AWS Glue: Serverless ETL jobs and data cataloging for schema discovery.

Amazon Athena: Serverless query engine using standard SQL to interface with S3 data directly.

Apache Parquet: Columnar storage optimized for analytical performance and reduced query costs.

**2. Data Processing**

Pandas: Localized processing and structural mapping for early-stage raw data.

SQL: Relational logic applied at the Athena/Glue level for Silver and Gold layer definitions.

**3. AI Integration**

Claude API: Core LLM utilized for semantic reasoning and dynamic SQL translation.

SQL-RAG Architecture: Retrieval mechanism bridging natural language inputs with structured relational databases.

Natural Language to SQL Generation: Direct interpretation of unstructured Vietnamese queries into executable SQL commands over the Gold layer.

**Key Design Decisions**

**1. Layered Medallion Architecture**

Structuring data into explicit Bronze, Silver, and Gold boundaries improves validation gates, system error isolation, and historical data replayability if upstream website structures change.

**2. Serverless Cloud Infrastructure**

Using AWS Glue, S3, and Athena removes server management overhead. This pay-per-query model keeps infrastructure maintenance minimal and cost-efficient for medium-scale data warehousing.

**3. Structured SQL-RAG vs. Vector Search**

Standard vector embeddings can struggle with precise numeric filters (e.g., exact pricing or specific star counts). Because hotel data is strictly relational, semantic queries are converted into deterministic SQL clauses.

Example: > "Find 5-star hotels in Da Nang near the beach with rating above 4.5" > maps directly to exact SQL WHERE conditions, removing the ambiguity of pure semantic distance vector calculations.

**4. Conversational LLM Interface**

The pipeline integrates Claude to handle context retention across multi-turn user interactions, translating natural language requests into structured SQL scripts without exposing database administrative layers.

**Platform Deliverables**

Automated target platform scraping pipeline with built-in schema resilience.

Production-ready scheduling for recurring incremental data updates.

Centralized, structured historical database of hotels in Vietnam.

Serverless cloud-native storage layout built on optimal data formats (Parquet).

Intent-driven, text-to-SQL search pipeline interface.

**Project Structure**

Plaintext

<img width="920" height="443" alt="Screenshot 2026-07-09 at 11 28 22 PM" src="https://github.com/user-attachments/assets/8f487a80-6569-4525-9a24-8fe5f795c8eb" />

**Future Improvements**

Transition from batch orchestration to real-time stream ingestion (AWS Kinesis / Apache Kafka).

Integrate data quality testing and threshold alerting frameworks (Great Expectations).

Set up automated CI/CD workflows for validation profiling of ETL code changes.

Scale ingestion targets to include regional flight and tourism platform metrics.

**Author**

Personal Data Engineering Project Developed to implement and benchmark:

Cloud-Native ETL Pipeline Architectures

Automated Scheduling and Distributed Orchestration

Analytical Data Modeling & Storage Strategy

Relational Text-to-SQL Application Delivery
