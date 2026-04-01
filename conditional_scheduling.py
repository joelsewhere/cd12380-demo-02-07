from airflow.sdk import dag, asset, Asset, task
from airflow.sdk.definitions.asset import AssetAny, AssetAll
from datetime import datetime


@asset(
        schedule="@daily",
        uri="file://workspace/external_storage/dataset1.csv",
        group="engineering"
        )
def asset_1():

    return ['metadata']

@asset(
        schedule="@daily",
        uri="file://workspace/external_storage/dataset2.csv",
        group="data_science"
        )
def asset_2():

    return ['metadata']


# --- Consumer: triggers when EITHER asset updates ---
@dag(schedule=(asset_1 | asset_2))
def any_consumer():

    @task
    def handle_any(ti, triggering_asset_events):
        from textwrap import dedent
        
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(dedent(f"""
                f"Asset: {asset.uri}"
                Source DAG: {event.source_dag_id}
                Event Timestamp: {event.timestamp}
                """)
                )
                metadata = ti.xcom_pull(
                    dag_id=event.source_dag_id,
                    task_id=event.source_task_id
                )
                print(f'Metadata: {metadata}')


    handle_any()


# --- Consumer: triggers only when BOTH assets have updated ---
@dag(schedule=(asset_1 & asset_2))
def all_consumer():

    @task
    def handle_all(ti, triggering_asset_events):
        from textwrap import dedent
        
        for asset, events in triggering_asset_events.items():
            for event in events:
                print(dedent(f"""
                f"Asset: {asset.uri}"
                Source DAG: {event.source_dag_id}
                Event Timestamp: {event.timestamp}
                """)
                )
                metadata = ti.xcom_pull(
                    dag_id=event.source_dag_id,
                    task_id=event.source_task_id
                )
                print(f'Metadata: {metadata}')

    handle_all()