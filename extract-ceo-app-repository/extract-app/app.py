#------------------------------   Este es un código demostrativo y no está completo por razones de confidencialidad -----------------------------------

import os
from flask import Flask

import requests
import datetime
import json
from dateutil.relativedelta import relativedelta
from google.cloud import storage
from dateutil import parser
import pytz


app = Flask(__name__)

def parse_and_format_date(date_str):
    if date_str:
        try:
            date_object = parser.parse(date_str)
            return date_object.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    else:
        return None

def parse_and_format_date_only(date_str):
    if date_str:
        try:
            date_object = parser.parse(date_str)
            return date_object.strftime('%Y-%m-%d')  # Solo fecha, sin hora
        except ValueError:
            return None
    else:
        return None

def procesar_orden(orden):
    delivery_order_id = orden.get('delivery_order_id', '')
    code = orden.get('code', '')
    recycling = orden.get('recycling', False) if orden.get('recycling') not in [None, ""] else False
    assigned_courier = orden.get('assigned_courier', None) if orden.get('assigned_courier') not in [None, ""] else None
    size_box = "".join(char for char in orden.get('size_box', '') if not char.isdigit()) if orden.get('size_box') not in [None, ""] else None
    cross_docking_location_code = orden.get('cross_docking_location_code', None) if orden.get('cross_docking_location_code') not in [None, ""] else None
    delivery_attemps = orden.get('delivery_attemps', None) if orden.get('delivery_attemps') not in [None, ""] else None
    
    destination_geo_coding = orden.get('destination', {}).get('geo_coding', None) if orden.get('destination', {}).get('geo_coding') not in [None, ""] else None
    destination_geo_location_lon = orden.get('destination', {}).get('geo_location', {}).get('lon', None) if orden.get('destination', {}).get('geo_location', {}).get('lon') not in [None, ""] else None
    destination_geo_location_lat = orden.get('destination', {}).get('geo_location', {}).get('lat', None) if orden.get('destination', {}).get('geo_location', {}).get('lat') not in [None, ""] else None
    destination_local = orden.get('destination', {}).get('local', None) if orden.get('destination', {}).get('local') not in [None, ""] else None
    destination_not_located = orden.get('destination', {}).get('not_located', None) if orden.get('destination', {}).get('not_located') not in [None, ""] else None
    destination_number = orden.get('destination', {}).get('number', None) if orden.get('destination', {}).get('number') not in [None, ""] else None
    destination_street = orden.get('destination', {}).get('street', None) if orden.get('destination', {}).get('street') not in [None, ""] else None
    destination_structure_id = orden.get('destination', {}).get('structure_id', None) if orden.get('destination', {}).get('structure_id') not in [None, ""] else None
    destination_polygon = orden.get('destination', {}).get('polygon', None) if 'polygon' in orden.get('destination', {}) and orden.get('destination', {}).get('polygon') not in [None, ""] else None
    
    # Transformación para lograr destination_polygon_lab
    destination_polygon_lab = None
    if destination_structure_id in [13123, 13120, 13101]:
        params = {"lat": destination_geo_location_lat, "lon": destination_geo_location_lon, "structure_id": destination_structure_id}
        response = requests.post('https://pickup.alasxpress.com/api/_polygon-finder-lab', json=params)
        if response.status_code == 200:
            destination_polygon_lab = json.loads(response.content)
            destination_polygon_lab = destination_polygon_lab if 'contained' in destination_polygon_lab and destination_polygon_lab['contained'] else None
            if destination_polygon_lab is not None:
                destination_polygon_lab = destination_polygon_lab.get('segmentation')


    #Siguen Otras transformaciones sucesivamente ...
    

    datos_orden = {
        "delivery_order_id": delivery_order_id,
        "code": code,
        "recycling": recycling,
        "assigned_courier": assigned_courier,
        "size_box": size_box,
        "cross_docking_location_code": cross_docking_location_code,
        "delivery_attemps": delivery_attemps,
        "destination_geo_coding": destination_geo_coding,
        "destination_geo_location_lon": destination_geo_location_lon,
        "destination_geo_location_lat": destination_geo_location_lat,
        "destination_local": destination_local,
        "destination_not_located": destination_not_located,
        "destination_number": destination_number,
        "destination_street": destination_street,
        "destination_structure_id": destination_structure_id,
        "destination_polygon": destination_polygon,
        "destination_polygon_lab": destination_polygon_lab,
        "dispatcher_employee_code": dispatcher_employee_code,
        "equipment_serial": equipment_serial,
        "equipment_serial_2nd": equipment_serial_2nd,
        "extended_info_documents": extended_info_documents,
        "extended_info_integration": extended_info_integration,
        # continuan más campos de la orden alrededor de 100
    
    }

    return datos_orden

def hacer_peticion(date, page_number, auth, max_reintentos=3):
    url = "https://url-de-la-api/sistema-tercerizado"
    body = {
        "date_type": 9,
        "date_from": date.strftime("%Y-%m-%d"),
        "date_to": date.strftime("%Y-%m-%d"),
        "page_number": page_number,
        "page_size": 100,
        "postgresql": False
    }

    for intento in range(max_reintentos):
        try:
            response = requests.post(url, json=body, auth=auth)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error en la petición: {e}, intento {intento + 1} de {max_reintentos}.")
            if intento == max_reintentos - 1:
                print("Se alcanzó el máximo número de intentos. Continuando con la siguiente petición...")
                return None  # Devuelve None para indicar el fallo

def guardar_en_gcs(data, bucket_name, prefix, filename, execution_date):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"{execution_date}/{prefix}/{filename}")

    # Convertir los datos a JSON y luego a bytes con codificación UTF-8
    data_json = json.dumps(data)
    data_encoded = data_json.encode('utf-8')

    blob.upload_from_string(data_encoded, content_type='application/json')
    #print(f"Datos guardados en GCS: {prefix}/{filename}")
    print(f"Datos almacenados en GCS: {prefix}/{filename}")

@app.route('/')
def main():
    try:
        santiago_tz = pytz.timezone('America/Santiago')

        auth = ('xxxxxxxxx', 'zzzzzzzzzzzzzzzz')  
        bucket_name = 'nombre_bucket_GCS'
        fecha_final = datetime.datetime.now(santiago_tz) - relativedelta(months=5)
        fecha_actual = datetime.datetime.now(santiago_tz) - datetime.timedelta(days=1)
        execution_date = datetime.datetime.now(santiago_tz).strftime("%Y-%m-%d")
        fallos = []

        while fecha_actual > fecha_final:
            datos = []
            page_number = 0
            while True:
                respuesta = hacer_peticion(fecha_actual, page_number, auth)
                if respuesta is None:
                    fallos.append((fecha_actual.strftime("%Y-%m-%d"), page_number))
                    break

                for orden in respuesta.get('items', []):
                    datos.append(procesar_orden(orden))

                if len(datos) >= respuesta.get('total', 0):
                    break
                page_number += 1

            fecha_str = fecha_actual.strftime("%Y-%m-%d")
            guardar_en_gcs(datos, bucket_name, fecha_str, f"{fecha_str}.json", execution_date)

            metadata = {
                "fecha": fecha_str,
                "total_ordenes": len(datos),
                "fallos": fallos
            }
            guardar_en_gcs(metadata, bucket_name, fecha_str, f"{fecha_str}_metadata.json", execution_date)

            fecha_actual -= datetime.timedelta(days=1)
        
        print("Proceso de Extracción completado exitosamente")
        return "Proceso de extracción completado exitosamente", 200  
        
    except Exception as e:
        return f"Error al ejecutar el proceso: {str(e)}", 500  



    

if __name__ == "__main__":
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')
