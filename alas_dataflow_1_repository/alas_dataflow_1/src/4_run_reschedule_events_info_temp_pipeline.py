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

def process_reschedule_events_info(record):
    
    # método anidado para que lo pueda reconocer ya que afuera no lo reconoce
    def parse_timestamp(timestamp_str):
        from datetime import datetime  
        if timestamp_str:
            try:
                return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f').isoformat()
            except ValueError:
                return None
        return None
    
    if 'reschedule_events_info_json' not in record or record['reschedule_events_info_json'] is None:
        return

    events = json.loads(record['reschedule_events_info_json'])
    for event in events:
        # Extrae y transforma los campos necesarios del evento
        info = event.get('info', {})
        yield {
            'status': event.get('status'),
            'timestamp': parse_timestamp(event.get('timestamp')),
            'info_user_name': info.get('user_name'),            
            
            'info_old_packaging_expected': parse_timestamp(info.get('old_packaging_expected')),
            'info_new_packaging_expected': info.get('new_packaging_expected'),
            'info_old_b2c_delivery_expected': info.get('old_b2c_delivery_expected'),
            'info_new_b2c_delivery_expected': parse_timestamp(info.get('new_b2c_delivery_expected')),
            'info_old_b2b_delivery_expected': info.get('old_b2b_delivery_expected'),
            'info_new_b2b_delivery_expected': info.get('new_b2b_delivery_expected'),
            'info_rescheduled_comments': info.get('rescheduled_comments'),
            
            'delivery_order_id': record['delivery_order_id']            
        }
        
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

def run_reschedule_events_info_temp_pipeline(file_list):
    pipeline_options = get_pipeline_options()

    # Define aquí el esquema de la tabla events_info_temp como una cadena
    schema_reschedule_events_info_temp_str = ','.join([
        "status:INTEGER",
        "timestamp:TIMESTAMP",
        "info_user_name:STRING",
        "info_old_packaging_expected:TIMESTAMP",
        "info_new_packaging_expected:TIMESTAMP",
        "info_old_b2c_delivery_expected:TIMESTAMP",
        "info_new_b2c_delivery_expected:TIMESTAMP",
        "info_old_b2b_delivery_expected:TIMESTAMP",
        "info_new_b2b_delivery_expected:TIMESTAMP",
        "info_rescheduled_comments:STRING",
        "delivery_order_id:STRING"
    ])

    with beam.Pipeline(options=pipeline_options) as p:
        for file_path in file_list:
            base_name = os.path.basename(file_path)
            (p
             | f'MatchFilePatternEvents_{base_name}' >> fileio.MatchFiles(file_path)
             | f'ReadMatchesEvents_{base_name}' >> fileio.ReadMatches()
             | f'ProcessJsonFileEvents_{base_name}' >> beam.FlatMap(process_json_file)
             | f'ExtraerYTransformarEventos_{base_name}' >> beam.FlatMap(process_reschedule_events_info)
             | f'EscribirABigQueryEventsInfo_{base_name}' >> beam.io.WriteToBigQuery(
                 f'{PROJECT_ID}:{DATASET}.reschedule_events_info_temp',
                 schema=schema_reschedule_events_info_temp_str,
                 method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )        

            
#-----------       EJECUCIÓN PRINCIPAL       --------   

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Iniciando el pipeline de Beam 4_run_reschedule_events_info_temp_pipeline...")
    
    santiago_tz = pytz.timezone('America/Santiago')
    now_in_santiago = datetime.now(santiago_tz)
    today_folder = now_in_santiago.strftime('%Y-%m-%d') + '/'
    logging.info(f"today_folder:{today_folder}")
        
    file_list = list_files(BUCKET_NAME, today_folder)    
    logging.info(f"valor de file_list: {file_list}")   
    
    logging.info("Inicia el pipeline 4_run_reschedule_events_info_temp_pipeline...")
    run_reschedule_events_info_temp_pipeline(file_list)
    
    logging.info("============   Fin de 4_run_reschedule_events_info_temp_pipeline   ============")

