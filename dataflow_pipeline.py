import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "project-c18eeebc-1b19-4616-80b"

SUBSCRIPTION = (
    "projects/project-c18eeebc-1b19-4616-80b/"
    "subscriptions/clinical-data-events-sub"
)

BIGQUERY_TABLE = (
    "project-c18eeebc-1b19-4616-80b:clinical_data.raw_fhir_events"
)


BQ_SCHEMA = {
    "fields": [
        {"name": "resource_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "resource_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "patient_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "raw_fhir_json", "type": "JSON", "mode": "NULLABLE"},
    ]
}


def parse_fhir_message(message):
    try:
        resource = json.loads(message.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    resource_type = resource.get("resourceType")
    resource_id = resource.get("id")

    if not resource_type or not resource_id:
        return None

    if resource_type == "Patient":
        patient_id = resource_id
    else:
        patient_id = None

        subject = resource.get("subject", {})
        patient = resource.get("patient", {})

        reference = subject.get("reference") or patient.get("reference")

        if reference and "/" in reference:
            patient_id = reference.split("/")[-1]

    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "patient_id": patient_id,
        "event_timestamp": datetime.now(timezone.utc),
        "raw_fhir_json": resource,
    }


def run():
    pipeline_options = PipelineOptions(
        streaming=True,
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub"
            >> ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse FHIR JSON"
            >> beam.Map(parse_fhir_message)
            | "Filter invalid messages"
            >> beam.Filter(lambda x: x is not None)
            | "Write to BigQuery"
            >> WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                type_overrides={"JSON": dict},
            )
        )


if __name__ == "__main__":
    run()