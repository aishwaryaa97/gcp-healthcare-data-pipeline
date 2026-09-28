# GCP Healthcare Data Pipeline

A hands-on Google Cloud data engineering project using the Synthea synthetic healthcare dataset in FHIR/JSON format.

## Project Overview

This project demonstrates a cloud-based healthcare data pipeline using Google Cloud Platform (GCP).

The dataset contains synthetic healthcare records represented as FHIR/JSON resources, including resources such as:

- Patient
- Encounter

The project explores both batch data ingestion and streaming data processing workflows.

## Architecture

### Batch Ingestion

Synthea FHIR/JSON
→ Cloud Storage
→ BigQuery

### Streaming Workflow

Synthea FHIR/JSON
→ Python Producer
→ Pub/Sub
→ Dataflow / Apache Beam

## Technologies

- Google Cloud Platform (GCP)
- Cloud Storage
- BigQuery
- Pub/Sub
- Dataflow
- Apache Beam
- Python
- SQL
- FHIR / JSON
- Synthea

## Project Components

### 1. Synthea Dataset

Synthetic healthcare data generated in FHIR/JSON format.

### 2. Cloud Storage

Used as the cloud storage/staging layer for the healthcare JSON files.

### 3. BigQuery

Used to create datasets and tables for storing structured healthcare data for analytics.

### 4. Pub/Sub

Used to publish healthcare records/messages as part of the streaming workflow.

### 5. Dataflow

A Dataflow pipeline using Apache Beam was developed and tested to process messages from Pub/Sub.

### 6. Python

Python was used to parse the healthcare JSON data and publish records to Pub/Sub.

## Batch vs Streaming

### Batch

Raw healthcare JSON data was staged in Cloud Storage and loaded into BigQuery.

### Streaming

A Python producer published healthcare records to Pub/Sub, which were processed through a Dataflow / Apache Beam streaming pipeline.

## Learning Outcomes

Through this project, I practiced:

- GCP data pipeline design
- Batch data ingestion
- Streaming data processing
- Pub/Sub messaging
- Dataflow and Apache Beam
- BigQuery data storage
- Cloud Storage
- Python-based data ingestion
- Working with FHIR/JSON healthcare data
