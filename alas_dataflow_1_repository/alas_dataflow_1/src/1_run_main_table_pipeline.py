#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import pytz
from google.cloud import storage
import os
import json
import logging
from apache_beam.io import fileio


# Configuraciones
PROJECT_ID    = 'PROJECT_ID'
BUCKET_NAME   = 'BUCKET_NAME'
DATASET       = 'nombre_dataset'
TEMP_LOCATION = 'gs://nombre_bucket/temp'
RUNNER_TYPE   = 'DataflowRunner'


#-----------       MÉTODOOS SECUNDARIOS       --------
def list_files(bucket_name, prefix):
    storage_client = storage.Client()
    files = []

    # Lista todas las subcarpetas en la carpeta del día actual
    subfolders = list_subfolders(bucket_name, prefix)

    # Para cada subcarpeta, lista todos los archivos JSON que no son de metadatos
    for subfolder in subfolders:
        blobs = storage_client.list_blobs(bucket_name, prefix=subfolder)

        for blob in blobs:
            if blob.name.endswith('.json') and not 'metadata' in blob.name:
                files.append(f'gs://{bucket_name}/{blob.name}')

    return files

def list_subfolders(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')

    subfolders = []
    for page in blobs.pages:
        for prefix in page.prefixes:
            subfolders.append(prefix)
    
    return subfolders

def process_json_file(readable_file):
    try:
        with readable_file.open() as file:
            json_content = json.load(file)
            for record in json_content:
                # Convertir ciertos campos a formato JSON si no son None
                for json_field in ["events_info_json", "schedule_events_info_json", 
                                   "reschedule_events_info_json", "changes_info_json", 
                                   "packages_json", "items_json", "extended_info_documents", "statuses"]:
                    if record.get(json_field) is not None:
                        record[json_field] = json.dumps(record[json_field])
                yield record
    except Exception as e:
        logging.error(f"Error procesando el archivo {readable_file.metadata.path}: {e}")
        
def get_pipeline_options():
    if RUNNER_TYPE == 'DirectRunner':
        return PipelineOptions(
            runner=RUNNER_TYPE,
            project=PROJECT_ID,
            temp_location=TEMP_LOCATION
        )
    elif RUNNER_TYPE == 'DataflowRunner':
        return PipelineOptions(
            runner=RUNNER_TYPE,
            project=PROJECT_ID,
            staging_location=TEMP_LOCATION,
            temp_location=TEMP_LOCATION,
            region='us-central1',
            job_name='job-transform-gcs-to-bigquery',
            max_num_workers=10,
            worker_machine_type='n2-standard-4',
        )
    else:
        raise ValueError("Tipo de runner no soportado")
    
def run_main_table_pipeline(file_list):
    pipeline_options = get_pipeline_options()

    # Define el esquema de la tabla delivery_order_master como una cadena
    schema_delivery_order_master_str = ','.join([
        "delivery_order_id:STRING",
        "recycling:BOOLEAN",
        "assigned_courier:STRING",
        "size_box:STRING",
        "code:STRING",
        "cross_docking_location_code:STRING",
        "delivery_attemps:INTEGER",
        "destination_geo_coding:STRING",
        "destination_geo_location_lon:FLOAT",
        "destination_geo_location_lat:FLOAT",
        "destination_local:STRING",
        "destination_not_located:BOOLEAN",
        "destination_number:STRING",
        "destination_street:STRING",
        "destination_structure_id:INTEGER",
        "destination_polygon:INTEGER",
        "dispatcher_employee_code:STRING",
        "equipment_serial:STRING",
        "equipment_serial_2nd:STRING",
       #continuan otros campos
    ])
    
          
    with beam.Pipeline(options=pipeline_options) as p:
        for file_path in file_list:
            (p
             | f'MatchFilePattern_{os.path.basename(file_path)}' >> fileio.MatchFiles(file_path)
             | f'ReadMatches_{os.path.basename(file_path)}' >> fileio.ReadMatches()
             | f'ProcessJsonFile_{os.path.basename(file_path)}' >> beam.FlatMap(process_json_file)
             | f'Escribir a BigQuery_{os.path.basename(file_path)}' >> beam.io.WriteToBigQuery(
                 f'{PROJECT_ID}:{DATASET}.temp_delivery_order_master',
                 schema=schema_delivery_order_master_str,
                 method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )

            
#-----------       EJECUCIÓN PRINCIPAL       --------   

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Iniciando el pipeline de Beam 1_run_main_table_pipeline...")
    
    santiago_tz = pytz.timezone('America/Santiago')
    now_in_santiago = datetime.now(santiago_tz)
    today_folder = now_in_santiago.strftime('%Y-%m-%d') + '/'
    logging.info(f"today_folder:{today_folder}")
        
    file_list = list_files(BUCKET_NAME, today_folder)    
    logging.info(f"valor de file_list: {file_list}")
    
    logging.info("Inicia el pipeline 1_run_main_table_pipeline...")
    run_main_table_pipeline(file_list)
   
    logging.info("============   Fin de 1_run_main_table_pipeline   ============")

