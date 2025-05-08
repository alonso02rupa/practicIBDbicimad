"""
Script para la Access Zone del Data Lake.
Lee datos desde la process-zone, realiza enriquecimiento (uniones, cálculos, nuevas columnas),
y los sube a la access-zone en formato Parquet.
"""

from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation
)
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
from pathlib import Path



# 1: Enriquecimiento de datos de parkings
def columnas_adicionales_ext(df):
    # Añadimos distrito_id y nombre_distrito
    df["distrito_id"] = [1, 1, 1, 4, 1, 1, 1, 7, 4, 3, 5, 7, 7, 4, 7]
    df["nombre_distrito"] = ["Centro", "Centro", "Centro", "Salamanca", "Centro", "Centro", "Centro",
                             "Chamberí", "Salamanca", "Retiro", "Chamartín", "Chamberí", "Chamberí",
                             "Salamanca", "Chamberí"]
    return df

def join_parking_info(df_parking, df_ext):
    df_merged = pd.merge(df_parking, df_ext, on='aparcamiento_id', how='inner')
    df_merged['porcentaje_ocupacion'] = (df_merged['plazas_ocupadas'] / df_merged['capacidad_total']) * 100
    df_merged['nivel_congestion'] = np.where(df_merged['porcentaje_ocupacion'] < 50, 'Bajo',
                                            np.where(df_merged['porcentaje_ocupacion'] < 80, 'Medio', 'Alto'))
    return df_merged

# 2: Enriquecimiento de datos municipales
def join_municipal_data(df_estaciones, df_distritos):
    df_joined = pd.merge(
        df_estaciones,
        df_distritos,
        left_on='distrito_id',
        right_on='id',
        how='inner'
    )
    df_joined.drop(columns=['id'], inplace=True, errors='ignore')
    return df_joined

def main_access_zone():
    print("Starting data enrichment for Access Zone...")

    # Descargamos datos desde process-zone
    print("\nDownloading data from process-zone...")
    try:
        # Descargamos cada archivo individualmente para identificar el problemático
        print("Downloading parkings/cleaned_parking_rotation.parquet...")
        parkings_df = download_dataframe_from_minio(
            'process-zone',
            'parkings/cleaned_parking_rotation.parquet',
            format='parquet'
        )
        print("Downloading parkings/cleaned_parking_info.parquet...")
        ext_df = download_dataframe_from_minio(
            'process-zone',
            'parkings/cleaned_parking_info.parquet',
            format='parquet' 
        )
        print("Downloading municipal/distritos.parquet...")
        df_distritos = download_dataframe_from_minio(
            'process-zone',
            'municipal/distritos.parquet',
            format='parquet'  
        )
        print("Downloading municipal/estaciones_transporte.parquet...")
        df_estaciones = download_dataframe_from_minio(
            'process-zone',
            'municipal/estaciones_transporte.parquet',
            format='parquet'
        )
        print("Downloading bicimad/cleaned_bicimad.parquet...")
        df_bicimad = download_dataframe_from_minio(
            'process-zone',
            'bicimad/cleaned_bicimad.parquet',
            format='parquet'
        )
        print("Downloading trafico/cleaned_traffic.parquet...")
        df_traffic = download_dataframe_from_minio(
            'process-zone',
            'trafico/cleaned_traffic.parquet',
            format='parquet'
        )
        print("Data downloaded successfully")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return

    # Enriquecemos datos
    print("\nEnriching data...")
    try:
        # Parkings
        ext_enriched = columnas_adicionales_ext(ext_df)
        parking_merge = join_parking_info(parkings_df, ext_enriched)
        print("Parking data enriched with districts, occupancy, and congestion level")

        # Municipal
        municipal_joined = join_municipal_data(df_estaciones, df_distritos)
        print("Municipal data enriched with joined estaciones_transporte and distritos")
    except Exception as e:
        print(f"Error enriching data: {e}")
        return

    # Subimos datos enriquecidos a access-zone
    print("\nUploading enriched data to access-zone...")
    try:
        # Parkings
        upload_dataframe_to_minio(
            parking_merge,
            'access-zone',
            'parkings/enriched_parking.parquet',
            format='parquet', 
            metadata={
                'description': 'Enriched parking data',
                'primary_keys': [],
                'transformations': 'Merged parking rotation and external info, calculated occupancy and congestion level, added district columns'
            }
        )
        log_data_transformation(
            'process-zone', 'parkings/cleaned_parking_rotation.parquet + parkings/cleaned_parking_info.parquet',
            'access-zone', 'parkings/enriched_parking.parquet',
            'Merged and enriched parking data uploaded'
        )

        # Municipal
        upload_dataframe_to_minio(
            municipal_joined,
            'access-zone',
            'municipal/enriched_estaciones_distritos.parquet',
            format='parquet', 
            metadata={
                'description': 'Enriched municipal data: joined estaciones_transporte and distritos',
                'primary_keys': [],
                'transformations': 'Joined estaciones_transporte and distritos on distrito_id=id'
            }
        )
        log_data_transformation(
            'process-zone', 'municipal/distritos.parquet + municipal/estaciones_transporte.parquet',
            'access-zone', 'municipal/enriched_estaciones_distritos.parquet',
            'Joined municipal data enriched and stored'
        )

        # Bicimad
        upload_dataframe_to_minio(
            df_bicimad,
            'access-zone',
            'bicimad/cleaned_bicimad.parquet',
            format='parquet',
            metadata={
                'description': 'Cleaned Bicimad data',
                'primary_keys': [],
                'transformations': 'None'
            }
        )
        log_data_transformation(
            'process-zone', 'bicimad/cleaned_bicimad.parquet',
            'access-zone', 'bicimad/cleaned_bicimad.parquet',
            'Cleaned Bicimad data transferred'
        )

        # Traffic
        upload_dataframe_to_minio(
            df_traffic,
            'access-zone',
            'trafico/cleaned_traffic.parquet',
            format='parquet',
            metadata={
                'description': 'Cleaned traffic data',
                'primary_keys': [],
                'transformations': 'None'
            }
        )
        log_data_transformation(
            'process-zone', 'trafico/cleaned_traffic.parquet',
            'access-zone', 'trafico/cleaned_traffic.parquet',
            'Cleaned traffic data transferred'
        )
    except Exception as e:
        print(f"Error uploading data to access-zone: {e}")
        return

    print("\nAccess Zone enrichment complete!")
    print("Data enriched and saved in access-zone.")

if __name__ == "__main__":
    main_access_zone()