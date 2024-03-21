from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import subprocess
import os

# Definición de argumentos predeterminados
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Función para realizar la petición HTTP con autenticación
def make_authenticated_http_request():
    credentials, project = google.auth.default()
    audience = "https://url-de-la-cloud-run"
    token = id_token.fetch_id_token(Request(), audience)

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(audience, headers=headers)
    print(response.text)


# Definición de la DAG (OJO si quieres que se ejecute a las 2am de chile: en verano(UTC-3) tienes que poner '0 5 * * *' pero en invierno(UTC-4) tienes que poner '0 6 * * *'  )
with DAG(
    'cloud_run_5dataflows_sps6',
    default_args=default_args,
    description='Una DAG para hacer una petición autenticada a Cloud Run, 5 dataflows y sps a bigquery',
    schedule_interval='0 5 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tarea para hacer la petición HTTP autenticada
    task_http_request = PythonOperator(task_id='authenticated_http_request_task', python_callable=make_authenticated_http_request)
    
    # Tarea para ejecutar un código Beam
    beam_run_1 = BeamRunPythonPipelineOperator(
        task_id="beam_run_1_task",
        py_file="gs://ruta-bucket-dag-de-composer/dags/1_run_main_table_pipeline.py",
        py_requirements=["apache-beam[gcp]==2.53.0", "google-cloud-storage==2.13.0", "pytz==2023.3.post1"],
        py_interpreter="python3",
        py_system_site_packages=False,
    )    
    
    beam_run_2 = BeamRunPythonPipelineOperator(
        task_id="beam_run_2_task",
        py_file="gs://ruta-bucket-dag-de-composer/dags/2_run_events_info_temp_pipeline.py",
        py_requirements=["apache-beam[gcp]==2.53.0", "google-cloud-storage==2.13.0", "pytz==2023.3.post1"],
        py_interpreter="python3",
        py_system_site_packages=False,
    )    
   
    beam_run_3 = BeamRunPythonPipelineOperator(
        task_id="beam_run_3_task",
        py_file="gs://ruta-bucket-dag-de-composer/dags/3_run_schedule_events_info_temp_pipeline.py",
        py_requirements=["apache-beam[gcp]==2.53.0", "google-cloud-storage==2.13.0", "pytz==2023.3.post1"],
        py_interpreter="python3",
        py_system_site_packages=False,
    )    
    
    beam_run_4 = BeamRunPythonPipelineOperator(
        task_id="beam_run_4_task",
        py_file="gs://ruta-bucket-dag-de-composer/dags/4_run_reschedule_events_info_temp_pipeline.py",
        py_requirements=["apache-beam[gcp]==2.53.0", "google-cloud-storage==2.13.0", "pytz==2023.3.post1"],
        py_interpreter="python3",
        py_system_site_packages=False,
    )    
    
    beam_run_5 = BeamRunPythonPipelineOperator(
        task_id="beam_run_5_task",
        py_file="gs://ruta-bucket-dag-de-composer/dags/5_run_packages_temp_pipeline.py",
        py_requirements=["apache-beam[gcp]==2.53.0", "google-cloud-storage==2.13.0", "pytz==2023.3.post1"],
        py_interpreter="python3",
        py_system_site_packages=False,
    )    
    
    # Ejecutar procedimientos almacenados en BigQuery
    sp_delete_duplicates = BigQueryExecuteQueryOperator(
        task_id='delete_duplicates_task',
        sql='CALL `project_id.name_dataset.delete_duplicates`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )   
    
    sp_merge_temp_master = BigQueryExecuteQueryOperator(
        task_id='merge_temp_master_task',
        sql='CALL `project_id.name_dataset.merge_temp_master`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )    
    
    sp_INSERT_DELIVERY_ORDER_WORK_TEMP = BigQueryExecuteQueryOperator(
        task_id='INSERT_DELIVERY_ORDER_WORK_TEMP_task',
        sql='CALL `project_id.name_dataset.INSERT_DELIVERY_ORDER_WORK_TEMP`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_status = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_status_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_status`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_macro_status = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_macro_status_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_macro_status`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_lob = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_lob_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_lob`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_packages = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_packages_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_packages`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_structure = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_structure_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_structure`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_type_route = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_type_route_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_type_route`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    sp_update_delivery_order_work_route_name = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_route_name_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_route_name`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_status_tlmk = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_status_tlmk_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_status_tlmk`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_total = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_total_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_total`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_portability = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_portability_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_portability`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_insert_table_temp_1 = BigQueryExecuteQueryOperator(
        task_id='insert_table_temp_1_task',
        sql='CALL `project_id.name_dataset.insert_table_temp`(1);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_visit_1 = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_visit_1_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_visit`(1);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_insert_table_temp_2 = BigQueryExecuteQueryOperator(
        task_id='insert_table_temp_2_task',
        sql='CALL `project_id.name_dataset.insert_table_temp`(2);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    sp_update_delivery_order_work_visit_2 = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_visit_2_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_visit`(2);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_insert_table_temp_3 = BigQueryExecuteQueryOperator(
        task_id='insert_table_temp_3_task',
        sql='CALL `project_id.name_dataset.insert_table_temp`(3);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_visit_3 = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_visit_3_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_visit`(3);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_insert_table_temp_0 = BigQueryExecuteQueryOperator(
        task_id='insert_table_temp_0_task',
        sql='CALL `project_id.name_dataset.insert_table_temp`(0);',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_work_scheduled = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_work_scheduled_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_work_scheduled`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_insert_delivery_order_visit_order = BigQueryExecuteQueryOperator(
        task_id='insert_delivery_order_visit_order_task',
        sql='CALL `project_id.name_dataset.insert_delivery_order_visit_order`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_insert_delivery_order_work = BigQueryExecuteQueryOperator(
        task_id='insert_delivery_order_work_task',
        sql='CALL `project_id.name_dataset.insert_delivery_order_work`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_update_delivery_order_master_visit = BigQueryExecuteQueryOperator(
        task_id='update_delivery_order_master_visit_task',
        sql='CALL `project_id.name_dataset.update_delivery_order_master_visit`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_delete_temp_delivery_order_master = BigQueryExecuteQueryOperator(
        task_id='delete_temp_delivery_order_master_task',
        sql='CALL `project_id.name_dataset.delete_temp_delivery_order_master`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_delete_events_info_temp = BigQueryExecuteQueryOperator(
        task_id='delete_events_info_temp_task',
        sql='CALL `project_id.name_dataset.delete_events_info_temp`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_delete_schedule_events_info_temp = BigQueryExecuteQueryOperator(
        task_id='delete_schedule_events_info_temp_task',
        sql='CALL `project_id.name_dataset.delete_schedule_events_info_temp`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_delete_reschedule_events_info_temp = BigQueryExecuteQueryOperator(
        task_id='delete_reschedule_events_info_temp_task',
        sql='CALL `project_id.name_dataset.delete_reschedule_events_info_temp`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
    
    sp_delete_packages_temp = BigQueryExecuteQueryOperator(
        task_id='delete_packages_temp_task',
        sql='CALL `project_id.name_dataset.delete_packages_temp`();',
        use_legacy_sql=False,
        location='US', 
        gcp_conn_id='google_cloud_default'
    )
        
     
    
    
    # Definiendo la secuencia de las tareas
    (
    task_http_request >>
    
    beam_run_1 >>
    beam_run_2 >>
    beam_run_3 >>
    beam_run_4 >>
    beam_run_5 >>
    
    sp_delete_duplicates >>
    sp_merge_temp_master >> 
    sp_INSERT_DELIVERY_ORDER_WORK_TEMP >> 
    sp_update_delivery_order_work_status >> 
    sp_update_delivery_order_work_macro_status >>
    sp_update_delivery_order_work_lob >> 
    sp_update_delivery_order_work_packages >> 
    sp_update_delivery_order_work_structure >> 
    sp_update_delivery_order_work_type_route >>
    sp_update_delivery_order_work_route_name >> 
    sp_update_delivery_order_work_status_tlmk >>
    sp_update_delivery_order_work_total >>
    sp_update_delivery_order_work_portability >>
    sp_insert_table_temp_1 >>
    sp_update_delivery_order_work_visit_1 >>
    sp_insert_table_temp_2 >>
    sp_update_delivery_order_work_visit_2 >>
    sp_insert_table_temp_3 >>
    sp_update_delivery_order_work_visit_3 >>
    sp_insert_table_temp_0 >>
    sp_update_delivery_order_work_scheduled >>
    sp_insert_delivery_order_visit_order >>
    sp_insert_delivery_order_work >>
    sp_update_delivery_order_master_visit >>
    sp_delete_temp_delivery_order_master >>
    sp_delete_events_info_temp >>
    sp_delete_schedule_events_info_temp >>
    sp_delete_reschedule_events_info_temp >>
    sp_delete_packages_temp
    )