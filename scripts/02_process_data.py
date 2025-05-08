"""
Script para la Process Zone del Data Lake (modificado para evitar download_file_as_string_from_minio).
Lee datos desde la raw-ingestion-zone, realiza procesamiento (limpieza, estandarización),
y los sube a la process-zone en formato Parquet.
"""

from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation
)
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import sqlite3
from pathlib import Path
import re
from minio import Minio
import io

# Función para limpiar cadenas
def clean_text_column(text):
    if isinstance(text, str):
        # Reemplazamos caracteres no válidos
        return text.encode('utf-8', errors='replace').decode('utf-8')
    return text

# Procesamiento de datos de tráfico
def column_clean_traffic(df):
    df.drop(columns=['sensor_id', 'velocidad_media_kmh'], inplace=True, errors='ignore')

def date_format_traffic(df):
    df['hora'] = df['fecha_hora'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S').time())
    df.drop(columns=['fecha_hora'], inplace=True, errors='ignore')

# Procesamiento de datos de BiciMAD
def column_clean_bicimad(df):
    df.drop(columns=['usuario_id', 'fecha_hora_inicio', 'fecha_hora_fin', 'duracion_segundos', 'distancia_km',
                     'calorias_estimadas', 'co2_evitado_gramos'], inplace=True, errors='ignore')

# Procesamiento de datos de parkings
def column_clean_parkings(df):
    df['fecha'] = df['fecha'].to_datetime()
    df['dia_semana'] = df['fecha'].dt.day_name()
    df.drop(columns=['plazas_libres', 'porcentaje_ocupacion'], inplace=True, errors='ignore')

def column_clean_ext(df):
    df.drop(columns=['nombre', 'direccion', 'plazas_movilidad_reducida', 'plazas_vehiculos_electricos',
                     'horario', 'tarifa_hora_euros', 'latitud', 'longitud'], inplace=True, errors='ignore')

# Procesamiento de scripts SQL
def preprocess_sql_script(script):
    script = script.replace("'Donnell", "''Donnell")
    # Reemplazar caracteres problemáticos
    script = script.encode('utf-8', errors='replace').decode('utf-8')
    return script

def download_sql_file(bucket_name: str, object_name: str, local_path: str):
    """
    Descarga un archivo SQL desde MinIO y lo guarda en disco.

    Args:
        bucket_name (str): Nombre del bucket en MinIO.
        object_name (str): Ruta del objeto en el bucket.
        local_path (str): Ruta local donde guardar el archivo.
    """
    try:
        client = Minio(
            endpoint="minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        client.fget_object(bucket_name, object_name, local_path)
    except Exception as e:
        raise Exception(f"Error downloading {object_name} from {bucket_name}: {e}")

def main_process_zone():
    print("Starting data processing for Process Zone...")

    # Descargamos datos desde raw-ingestion-zone
    print("\nDownloading data from raw-ingestion-zone...")
    try:
        trafico_df = download_dataframe_from_minio('raw-ingestion-zone', 'trafico/trafico-horario.csv')
        bicimad_df = download_dataframe_from_minio('raw-ingestion-zone', 'bicimad/bicimad-usos.csv')
        parkings_df = download_dataframe_from_minio('raw-ingestion-zone', 'aparcamiento/parkings_rotacion.csv')
        ext_df = download_dataframe_from_minio('raw-ingestion-zone', 'aparcamiento/ext_aparcamientos_info.csv')
        # Descargamos SQL como archivo temporal
        sql_temp_path = "temp_dump-bbdd-municipal.sql"
        download_sql_file('raw-ingestion-zone', 'sql/dump-bbdd-municipal.sql', sql_temp_path)
        with open(sql_temp_path, 'r', encoding='iso-8859-1') as f:
            municipal_sql = f.read()
        print("Data downloaded successfully")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return

    # Procesamos datos
    print("\nProcessing data...")

    # Tráfico
    column_clean_traffic(trafico_df)
    date_format_traffic(trafico_df)
    # Limpiamos columnas de texto
    for col in trafico_df.select_dtypes(include=['object']).columns:
        trafico_df[col] = trafico_df[col].apply(clean_text_column)
    print("Traffic data cleaned and formatted")

    # BiciMAD
    column_clean_bicimad(bicimad_df)
    for col in bicimad_df.select_dtypes(include=['object']).columns:
        bicimad_df[col] = bicimad_df[col].apply(clean_text_column)
    print("Bicimad data cleaned")

    # Parkings
    column_clean_parkings(parkings_df)
    column_clean_ext(ext_df)
    for col in parkings_df.select_dtypes(include=['object']).columns:
        parkings_df[col] = parkings_df[col].apply(clean_text_column)
    for col in ext_df.select_dtypes(include=['object']).columns:
        ext_df[col] = ext_df[col].apply(clean_text_column)
    print("Parking data cleaned")

    # Municipal (SQL con SQLite)
    SQL_FILE = "dump-bbdd-municipal.sql"
    PROCESSED_DATA_PATH = "processed_sql"
    DB_PATH = "temp.db"
    Path(PROCESSED_DATA_PATH).mkdir(parents=True, exist_ok=True)
    TABLES_TO_SAVE = {"distritos", "estaciones_transporte"}

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        sql_script = preprocess_sql_script(municipal_sql)
        cursor.executescript(sql_script)
        conn.commit()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tablas creadas: {tables}")

        # Procesamos tablas
        if "distritos" in tables:
            df_distritos = pd.read_sql_query("SELECT * FROM distritos", conn)
            df_distritos = df_distritos[['id', 'nombre', 'densidad_poblacion']]
            # Limpiamos columnas de texto
            for col in df_distritos.select_dtypes(include=['object']).columns:
                df_distritos[col] = df_distritos[col].apply(clean_text_column)
            df_distritos.to_parquet(f"{PROCESSED_DATA_PATH}/distritos.parquet", index=False, engine='pyarrow')
            print("Tabla distritos limpiada y guardada")
        else:
            print("Tabla distritos no encontrada")
            conn.close()
            return

        if "estaciones_transporte" in tables:
            df_estaciones = pd.read_sql_query("SELECT * FROM estaciones_transporte", conn)
            df_estaciones = df_estaciones[['distrito_id', 'tipo']]
            # Limpiamos columnas de texto
            for col in df_estaciones.select_dtypes(include=['object']).columns:
                df_estaciones[col] = df_estaciones[col].apply(clean_text_column)
            df_estaciones.to_parquet(f"{PROCESSED_DATA_PATH}/estaciones_transporte.parquet", index=False, engine='pyarrow')
            print("Tabla estaciones_transporte limpiada y guardada")
        else:
            print("Tabla estaciones_transporte no encontrada")
            conn.close()
            return

        conn.close()

    except sqlite3.OperationalError as e:
        print(f"Error al ejecutar el script SQL: {e}")
        lines = sql_script.splitlines()
        print("\nÚltimas líneas del script ejecutado:")
        print('\n'.join(lines[max(0, len(lines)-10):]))
        conn.close()
        return

    # Subimos datos procesados a process-zone
    print("\nUploading processed data to process-zone...")
    try:
        # Tráfico
        upload_dataframe_to_minio(
            trafico_df,
            'process-zone',
            'trafico/cleaned_traffic.parquet',
            format='parquet',
            metadata={
                'description': 'Cleaned and formatted traffic data',
                'primary_keys': [],
                'transformations': 'Dropped unnecessary columns, formatted date and time, cleaned text encoding'
            }
        )
        log_data_transformation(
            'raw-ingestion-zone', 'trafico-horarios.csv',
            'process-zone', 'trafico/cleaned_traffic.parquet',
            'Traffic data cleaned and converted to Parquet'
        )

        # BiciMAD
        upload_dataframe_to_minio(
            bicimad_df,
            'process-zone',
            'bicimad/cleaned_bicimad.parquet',
            format='parquet',
            metadata={
                'description': 'Cleaned BiciMAD data',
                'primary_keys': [],
                'transformations': 'Dropped unused columns, cleaned text encoding'
            }
        )
        log_data_transformation(
            'raw-ingestion-zone', 'bicimad-usos.csv',
            'process-zone', 'bicimad/cleaned_bicimad.parquet',
            'BiciMAD data cleaned and converted to Parquet'
        )

        # Parkings (rotación)
        upload_dataframe_to_minio(
            parkings_df,
            'process-zone',
            'parkings/cleaned_parking_rotation.parquet',
            format='parquet',
            metadata={
                'description': 'Cleaned parking rotation data',
                'primary_keys': [],
                'transformations': 'Dropped unused columns, cleaned text encoding'
            }
        )
        log_data_transformation(
            'raw-ingestion-zone', 'parkings-rotacion.csv',
            'process-zone', 'parkings/cleaned_parking_rotation.parquet',
            'Parking rotation data cleaned and converted to Parquet'
        )

        # Parkings (información externa)
        upload_dataframe_to_minio(
            ext_df,
            'process-zone',
            'parkings/cleaned_parking_info.parquet',
            format='parquet',
            metadata={
                'description': 'Cleaned external parking info',
                'primary_keys': [],
                'transformations': 'Dropped unused columns, cleaned text encoding'
            }
        )
        log_data_transformation(
            'raw-ingestion-zone', 'ext_aparcamientos_info.csv',
            'process-zone', 'parkings/cleaned_parking_info.parquet',
            'External parking info cleaned and converted to Parquet'
        )

        # Municipal (distritos y estaciones)
        for table in ['distritos', 'estaciones_transporte']:
            parquet_path = f"{PROCESSED_DATA_PATH}/{table}.parquet"
            df = pd.read_parquet(parquet_path)
            upload_dataframe_to_minio(
                df,
                'process-zone',
                f'municipal/{table}.parquet',
                format='parquet',
                metadata={
                    'description': f'Cleaned {table} data from municipal SQL',
                    'primary_keys': [],
                    'transformations': f'Filtered relevant columns from {table}, cleaned text encoding'
                }
            )
            log_data_transformation(
                'raw-ingestion-zone', 'dump-bbdd-municipal.sql',
                'process-zone', f'municipal/{table}.parquet',
                f'{table} data processed and stored'
            )

    except Exception as e:
        print(f"Error uploading data to process-zone: {e}")
        return

    print("\nProcess Zone processing complete!")
    print("Data cleaned, standardized, and saved in process-zone.")

if __name__ == "__main__":
    main_process_zone()