# Modern Distributed Data Lakehouse Using Apache Iceberg

This repository demonstrates the design and implementation of a highly scalable, cloud-native data lakehouse using Apache Iceberg—a modern open table format that brings ACID transactions, schema evolution, and time-travel queries to data lakes. It’s a hands-on showcase of building a production-ready data platform, combining modern data engineering tools and distributed computing principles, making it ideal for demonstrating real-world technical expertise in data architecture.

## Architecture Overview

The system is designed as a modular, **distributed data platform** supporting ingestion, storage, orchestration, and analytics:

- **Data Ingestion** – Handles streaming and batch data pipelines (simulated using Airflow DAGs).  
- **Storage Layer** – Iceberg tables on object storage (local S3 or MinIO) provide versioned, partitioned, and transactional datasets.  
- **Compute Layer** – Trino (formerly Presto) provides distributed SQL analytics over Iceberg tables for fast, scalable queries.  
- **Orchestration** – Apache Airflow manages ETL workflows and data pipeline dependencies.  
- **Containerized Environment** – Docker Compose ensures reproducible deployments across environments.


## Key Features

- **ACID-compliant data lakehouse** using Apache Iceberg.  
- **Time-travel queries** for historical data analysis.  
- **Schema evolution support**, enabling flexible and safe changes to datasets.  
- **Partitioned and versioned tables** for optimized query performance.  
- **Distributed SQL engine (Trino)** for scalable analytics.  
- **Containerized deployment** with Docker & Docker Compose for easy reproducibility.  
- **Airflow orchestration** for managing complex ETL workflows.

# Architecture Diagram

  <a href="https://github.com/temi109/Modern-Distributed-Data-Lakehouse-Using-Apache-Iceberg.git">
    <img src="https://github.com/temi109/Modern-Distributed-Data-Lakehouse-Using-Apache-Iceberg/blob/main/images/Modern%20Cloud-Native%20Data%20Lakehouse%20Architecture.png" alt="Logo" width="2000" height="2000">
  </a>


  
---

## Getting Started

### Prerequisites

- Docker & Docker Compose installed  
- Git installed  

### Installation

```bash
# Clone the repository
git clone https://github.com/temi109/Modern-Distributed-Data-Lakehouse-Using-Apache-Iceberg.git
cd Modern-Distributed-Data-Lakehouse-Using-Apache-Iceberg

# Start the full environment
docker-compose up -d
```

## Usage

1. Access Airflow UI at http://localhost:8080
2. Run the example DAGs to populate Iceberg tables
3. Query the tables using Trino at http://localhost:8080 (or via CLI)
4. Explore time-travel queries and schema evolution features

