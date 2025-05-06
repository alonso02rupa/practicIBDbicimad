"""
This script demonstrates data ingestion into the raw ingestion zone of the data lake.
It uploads data to MinIO's raw-ingestion-zone bucket in its native format.

Raw Ingestion Zone: Where data is stored in its original format without modifications.
"""
import pandas as pd
import os
from utils import upload_dataframe_to_minio, get_minio_client
from minio import Minio

def get_minio_client():
    client = Minio(
        "minio:9000",  # Correcto, coincide con el nombre del servicio en docker-compose.yml
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  # No es necesario usar HTTPS en este caso, ya que es HTTP
    )
    return client

def main():
    # Save sample data to local CSV files
    data_dir = '/data/raw-ingestion-zone'
    os.makedirs(data_dir, exist_ok=True)

    trafico_path = data_dir + "/trafico-horario.csv"
    usos_path = data_dir + "/bicimad-usos.csv"
    aparcamiento_path = data_dir + "/parkings-rotacion.csv"
    ext_aparcamiento_path = data_dir + "/ext_aparcamientos_info.csv"

    # Cargar los DataFrames desde los archivos CSV
    trafico_df = pd.read_csv(trafico_path)
    usos_df = pd.read_csv(usos_path)
    aparcamiento_df = pd.read_csv(aparcamiento_path)
    ext_aparcamiento_df = pd.read_csv(ext_aparcamiento_path)

    # Metadatos para cada dataset
    trafico_metadata = {
        'sensor_id': 'identificador del sensor',
        'fecha_hora': 'timestamp con fecha y hora del registro de información tomadas por los sensores',
        'total_vehiculos': 'suma total del número de vehículos que han pasado por los sensores en una hora',
        'coches': 'suma total de coches durante la hora',
        'motos': 'suma total de motos durante la hora',
        'camiones': 'suma total de camiones durante la hora',
        'buses': 'suma total de buses durante la hora',
        'nivel_congestion': 'en base a las previas medidas, el nivel de tráfico que se ha detectado'
    }

    usos_metadata = {
        'usuario_id': 'Identificador único del usuario que realiza la acción.',
        'tipo_usuario': 'Indica si el usuario es visitante, registrado, administrador, etc.',
        'recurso': 'Nombre o tipo del recurso que el usuario está utilizando (por ejemplo, "mazmorra", "monstruo").',
        'accion': 'Tipo de acción que realiza el usuario sobre el recurso (por ejemplo, "crear", "consultar", "modificar", "eliminar").',
        'fecha': 'Fecha y hora en que se registró el uso del recurso.'
    }

    aparcamiento_metadata = {
        "aparcamiento_id": "Identificador del aparcamiento. Es un número entero que distingue cada aparcamiento de forma única.",
        "fecha": "Fecha de la medición de ocupación. Está en formato 'YYYY-MM-DD'.",
        "hora": "Hora del día en la que se ha registrado la ocupación, expresada en formato de 24 horas (de 0 a 23).",
        "plazas_ocupadas": "Número de plazas de aparcamiento que están ocupadas en ese momento.",
        "plazas_libres": "Número de plazas que están libres en ese momento.",
        "porcentaje_ocupacion": "Porcentaje de ocupación del aparcamiento en esa hora específica. Se calcula como (plazas_ocupadas / (plazas_ocupadas + plazas_libres)) * 100."
    }

    ext_aparcamiento_metadata = {
        "aparcamiento_id": "Identificador del aparcamiento. Es un número entero que distingue cada aparcamiento de forma única.",
        "nombre": "Nombre del aparcamiento",
        "dirección": "Dirección física de la boca del aparcamiento",
        "capacidad_total": "Total de plazas de cualquier tipo localizadas en el aparcamiento",
        "plazas_movilidad_reducida": "Número de plazas para gente con movilidad reducida",
        "plazas_vehiculos_electricos": "Número de plazas con cargador incorporado",
        "tarifo_hora_euros": "Cantidad monetaria que cuesta mantener el coche en una plaza en euros",
        "horario": "Disponibilidad del aparcamiento",
        "latitud": "Latitud de la boca del aparcamiento",
        "longitud": "Longitud de la boca del aparcamiento"
    }

    # Subir los datos a MinIO, incluyendo el SQL
    # Subir el archivo SQL a MinIO desde la misma ruta
    dump_sql_path = os.path.join(data_dir, "dump-bbdd-municipal.sql")
    if os.path.exists(dump_sql_path):
        with open(dump_sql_path, 'rb') as file_data:
            client = get_minio_client()
            client.put_object(
                bucket_name='raw-ingestion-zone',
                object_name='sql/dump-bbdd-municipal.sql',
                data=file_data,
                length=os.path.getsize(dump_sql_path),
                content_type='application/sql'
            )

    upload_dataframe_to_minio(trafico_df, 'raw-ingestion-zone', 'trafico/trafico-horario.csv', metadata=trafico_metadata)
    upload_dataframe_to_minio(usos_df, 'raw-ingestion-zone', 'bicimad/bicimad-usos.csv', metadata=usos_metadata)
    upload_dataframe_to_minio(aparcamiento_df, 'raw-ingestion-zone', 'aparcamiento/parkings_rotacion.csv', metadata=aparcamiento_metadata)
    upload_dataframe_to_minio(ext_aparcamiento_df, 'raw-ingestion-zone', 'aparcamiento/ext_aparcamientos_info.csv', metadata=ext_aparcamiento_metadata)
    # Verificar los archivos en el bucket
    client = get_minio_client()
    print("\nVerifying uploaded files in raw-ingestion-zone:")
    objects = list(client.list_objects('raw-ingestion-zone', recursive=True))
    if objects:
        print(f"Files in raw-ingestion-zone: {[obj.object_name for obj in objects]}")
    else:
        print("No objects found in raw-ingestion-zone.")

    print("\nData ingestion into raw-ingestion-zone complete!")
    print("Note: The data in this zone is stored in its original format without modifications.")

if __name__ == "__main__":
    main()