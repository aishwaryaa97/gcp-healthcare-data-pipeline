# GCP Healthcare Data Pipeline

A hands-on Google Cloud data engineering project using the Synthea synthetic healthcare dataset in FHIR/JSON format.

## Project Overview

This project demonstrates a cloud-based healthcare data pipeline using Google Cloud Platform (GCP).

The dataset contains synthetic healthcare records represented as FHIR/JSON resources, including:

- Patient
- Encounter

The project explores both batch data ingestion and streaming data processing workflows.

## Pipeline Architecture

### 1. Batch Data Pipeline

```text
Synthea FHIR/JSON Files
        ↓
   Cloud Storage
        ↓
      Dataflow
   (Apache Beam)
        ↓
     BigQuery

```

### 2. Streaming Data Pipeline

```text

Synthea FHIR/JSON
        ↓
   Python Producer
        ↓
      Pub/Sub
        ↓
      Dataflow
   (Apache Beam)
        ↓
     BigQuery
```


### 3.Technologies Used

- Google Cloud Platform (GCP)
- Cloud Storage
- BigQuery
- Pub/Sub
- Dataflow
- Apache Beam
- Python
- SQL
- FHIR / JSON
- Synthea synthetic healthcare dataset



### 4.Key Data Engineering Concepts

- Batch data ingestion
- Streaming data processing
- Event-driven data ingestion using Pub/Sub
- FHIR/JSON parsing
- Data transformation using Apache Beam
- Cloud Storage integration
- BigQuery data loading
- Pipeline development using Python


### 5.Project Structure

gcp-healthcare-data-pipeline/
│
├── README.md
├── requirements.txt
├── producer.py
├── dataflow_pipeline.py
└── batch_dataflow_pipeline.py

### 6. Purpose

This project demonstrates practical experience building batch and streaming data pipelines on Google Cloud using Python, Apache Beam, Pub/Sub, Dataflow, Cloud Storage, and BigQuery.

