from utils import (
    download_dataframe_from_minio,
    log_data_transformation
)
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime

# Funciones de enriquecimiento
def columnas_adicionales_ext(df):
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
    print("Starting data enrichment and loading into PostgreSQL for Access Zone...")

    # Descargamos datos desde process-zone
    print("\nDownloading data from process-zone...")
    try:
        print("Downloading parkings/cleaned_parking_rotation.parquet...")
        parkings_df = download_dataframe_from_minio('process-zone', 'parkings/cleaned_parking_rotation.parquet', format='parquet')
        print("Downloading parkings/cleaned_parking_info.parquet...")
        ext_df = download_dataframe_from_minio('process-zone', 'parkings/cleaned_parking_info.parquet', format='parquet')
        print("Downloading municipal/distritos.parquet...")
        df_distritos = download_dataframe_from_minio('process-zone', 'municipal/distritos.parquet', format='parquet')
        print("Downloading municipal/estaciones_transporte.parquet...")
        df_estaciones = download_dataframe_from_minio('process-zone', 'municipal/estaciones_transporte.parquet', format='parquet')
        print("Downloading bicimad/cleaned_bicimad.parquet...")
        df_bicimad = download_dataframe_from_minio('process-zone', 'bicimad/cleaned_bicimad.parquet', format='parquet')
        print("Data downloaded successfully")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return

    # Enriquecemos datos
    print("\nEnriching data...")
    try:
        # Parkings (Objetivo 3)
        ext_enriched = columnas_adicionales_ext(ext_df)
        parking_merge = join_parking_info(parkings_df, ext_enriched)
        parking_merge['fecha_hora'] = pd.to_datetime(parking_merge['fecha'] + ' ' + parking_merge['hora'])
        print("Parking data enriched with districts, occupancy, congestion level, and fecha_hora")

        # Municipal (Objetivo 2)
        municipal_joined = join_municipal_data(df_estaciones, df_distritos)
        print("Municipal data enriched with joined estaciones_transporte and distritos")
    except Exception as e:
        print(f"Error enriching data: {e}")
        return

    # Conexión a PostgreSQL
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return

    # Creamos tablas de hechos y dimensiones de nuestro Data Warehouse
    try:
        # Dimensión Distritos
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_distritos (
            id INT PRIMARY KEY,
            nombre VARCHAR(255),
            densidad_poblacion FLOAT
        );
        """)

        # Dimensión Tipos de Usuario
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_tipos_usuario (
            id SERIAL PRIMARY KEY,
            tipo_usuario VARCHAR(50)
        );
        """)

        # Dimensión Tipos de Estación
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_tipos_estacion (
            id SERIAL PRIMARY KEY,
            tipo_estacion VARCHAR(50)
        );
        """)

        # Dimensión Aparcamientos
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_aparcamientos (
            id INT PRIMARY KEY,
            nombre VARCHAR(255),
            capacidad_total INT,
            distrito_id INT REFERENCES dim_distritos(id)
        );
        """)

        # Dimensión Fecha y Hora
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date_time (
            id SERIAL PRIMARY KEY,
            fecha_hora TIMESTAMP,
            fecha DATE,
            hora INT,
            dia_semana VARCHAR(10),
            numero_dia_semana INT,
            es_festivo BOOLEAN,
            mes INT,
            trimestre INT,
            año INT
        );
        """)

        # Hechos Usos BiciMAD
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_usos_bicimad (
            id_uso SERIAL PRIMARY KEY,
            estacion_origen_id INT,
            estacion_destino_id INT,
            tipo_usuario_id INT REFERENCES dim_tipos_usuario(id)
        );
        """)

        # Hechos Infraestructura
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_infraestructura (
            distrito_id INT REFERENCES dim_distritos(id),
            tipo_estacion_id INT REFERENCES dim_tipos_estacion(id),
            cantidad INT,
            PRIMARY KEY (distrito_id, tipo_estacion_id)
        );
        """)

        # Hechos Ocupación Parkings
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_ocupacion_parkings (
            aparcamiento_id INT REFERENCES dim_aparcamientos(id),
            date_time_id INT REFERENCES dim_date_time(id),
            plazas_ocupadas INT,
            porcentaje_ocupacion FLOAT,
            PRIMARY KEY (aparcamiento_id, date_time_id)
        );
        """)
        conn.commit()
        print("Tables created or already exist")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
        return

    # Insertamos los datos en las tablas de dimensiones
    try:
        # dim_distritos
        for _, row in df_distritos.iterrows():
            cur.execute("""
            INSERT INTO dim_distritos (id, nombre, densidad_poblacion)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (row['id'], row['nombre'], row['densidad_poblacion']))

        # dim_tipos_usuario
        tipos_usuario = df_bicimad['tipo_usuario'].unique()
        for tipo in tipos_usuario:
            cur.execute("""
            INSERT INTO dim_tipos_usuario (tipo_usuario)
            VALUES (%s)
            ON CONFLICT DO NOTHING;
            """, (tipo,))

        # dim_tipos_estacion
        tipos_estacion = municipal_joined['tipo'].unique()
        for tipo in tipos_estacion:
            cur.execute("""
            INSERT INTO dim_tipos_estacion (tipo_estacion)
            VALUES (%s)
            ON CONFLICT DO NOTHING;
            """, (tipo,))

        # dim_aparcamientos
        for _, row in ext_enriched.iterrows():
            cur.execute("""
            INSERT INTO dim_aparcamientos (id, nombre, capacidad_total, distrito_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (row['aparcamiento_id'], row.get('nombre', 'Unknown'), row['capacidad_total'], row['distrito_id']))

        # dim_date_time
        fechas_horas_unicas = parking_merge['fecha_hora'].unique()
        for fecha_hora in fechas_horas_unicas:
            dt = pd.Timestamp(fecha_hora)
            cur.execute("""
            INSERT INTO dim_date_time (fecha_hora, fecha, hora, dia_semana, numero_dia_semana, es_festivo, mes, trimestre, año)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """, (
                dt,
                dt.date(),
                dt.hour,
                dt.day_name(),
                dt.dayofweek,
                False,  # es_festivo se establece como False por falta de datos
                dt.month,
                (dt.month - 1) // 3 + 1,
                dt.year
            ))

        conn.commit()
        print("Dimensions populated")
    except Exception as e:
        print(f"Error populating dimensions: {e}")
        conn.rollback()
        return

    # Insertamos datos en las tablas de hechos
    try:
        # fact_usos_bicimad
        cur.execute("SELECT id, tipo_usuario FROM dim_tipos_usuario")
        tipos_usuario_dict = {row[1]: row[0] for row in cur.fetchall()}
        for _, row in df_bicimad.iterrows():
            cur.execute("""
            INSERT INTO fact_usos_bicimad (estacion_origen_id, estacion_destino_id, tipo_usuario_id)
            VALUES (%s, %s, %s);
            """, (row['estacion_origen'], row['estacion_destino'], tipos_usuario_dict[row['tipo_usuario']]))

        # fact_infraestructura
        cur.execute("SELECT id, tipo_estacion FROM dim_tipos_estacion")
        tipos_estacion_dict = {row[1]: row[0] for row in cur.fetchall()}
        infra_grouped = municipal_joined.groupby(['distrito_id', 'tipo']).size().reset_index(name='cantidad')
        for _, row in infra_grouped.iterrows():
            cur.execute("""
            INSERT INTO fact_infraestructura (distrito_id, tipo_estacion_id, cantidad)
            VALUES (%s, %s, %s)
            ON CONFLICT (distrito_id, tipo_estacion_id) DO UPDATE SET cantidad = EXCLUDED.cantidad;
            """, (row['distrito_id'], tipos_estacion_dict[row['tipo']], row['cantidad']))

        # fact_ocupacion_parkings
        cur.execute("SELECT id, fecha_hora FROM dim_date_time")
        date_time_dict = {row[1]: row[0] for row in cur.fetchall()}
        for _, row in parking_merge.iterrows():
            cur.execute("""
            INSERT INTO fact_ocupacion_parkings (aparcamiento_id, date_time_id, plazas_ocupadas, porcentaje_ocupacion)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (aparcamiento_id, date_time_id) DO NOTHING;
            """, (
                row['aparcamiento_id'],
                date_time_dict[row['fecha_hora']],
                row['plazas_ocupadas'],
                row['porcentaje_ocupacion']
            ))

        conn.commit()
        print("Fact tables populated")
    except Exception as e:
        print(f"Error populating fact tables: {e}")
        conn.rollback()
        return

    # Cerramos conexión
    cur.close()
    conn.close()
    print("PostgreSQL connection closed")

    print("\nAccess Zone enrichment and loading complete!")
    print("Data enriched and loaded into PostgreSQL data warehouse.")

if __name__ == "__main__":
    main_access_zone()