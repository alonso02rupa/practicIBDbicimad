"""
This script demonstrates querying the data in the access zone using different methods:
1. Direct Pandas querying from the access-zone
2. SQL queries via Trino for complex analytical queries
3. Example analytics and insights extracted from the data

Access Zone: Contains analytics-ready data for visualization, reporting, and advanced analytics.
"""
from utils import (
    download_dataframe_from_minio,
    execute_trino_query
)
import pandas as pd
import matplotlib.pyplot as plt
import io

def generate_insights(parking_data):
    print("\n=== Parking Occupation Insights ===")

    parking_data['fecha'] = pd.to_datetime(parking_data['fecha'])
    parking_data['dia_semana'] = parking_data['fecha'].dt.day_name()

    # Agrupamos ocupación por aparcamiento, hora y día de la semana
    ocupacion_stats = parking_data.groupby(['aparcamiento_id', 'hora', 'dia_semana'])['porcentaje_ocupacion'].agg(['mean', 'std']).reset_index()
    ocupacion_stats.columns = ['aparcamiento_id', 'hora', 'dia_semana', 'media_ocupacion', 'variabilidad_ocupacion']

    # Promediamos la variabilidad diaria y semanal por aparcamiento
    variabilidad_aparcamiento = ocupacion_stats.groupby('aparcamiento_id')['variabilidad_ocupacion'].mean().reset_index()
    variabilidad_aparcamiento.columns = ['aparcamiento_id', 'variabilidad_media']

    # Unimos con metadata del aparcamiento
    aparcamientos_analisis = pd.merge(parking_data, variabilidad_aparcamiento, on='aparcamiento_id')

    # Top 5 aparcamientos con mayor variabilidad
    print("\nInsight 1: Aparcamientos con mayor variabilidad de ocupación")
    print(aparcamientos_analisis.sort_values('variabilidad_media', ascending=False)[
        ['nombre', 'direccion', 'variabilidad_media']
    ].head(5))

    # Posible relación con ubicación geográfica 
    # Creamos bins por latitud y longitud
    aparcamientos_analisis['zona'] = pd.cut(aparcamientos_analisis['latitud'], bins=3).astype(str) + " | " + pd.cut(aparcamientos_analisis['longitud'], bins=3).astype(str)

    zona_variabilidad = aparcamientos_analisis.groupby('zona')['variabilidad_media'].mean().reset_index().sort_values('variabilidad_media', ascending=False)

    print("\nInsight 2: Zonas con mayor variabilidad promedio")
    print(zona_variabilidad)

    # Recomendaciones
    print("\nInsight 3: Recomendaciones")
    zona_top = zona_variabilidad.iloc[0]['zona']
    top_aparcamiento = aparcamientos_analisis.sort_values('variabilidad_media', ascending=False).iloc[0]['nombre']
    
    print(f"1. Monitorizar el aparcamiento '{top_aparcamiento}', con alta variabilidad de ocupación, para ajustar precios dinámicos o promociones.")
    print(f"2. La zona {zona_top} presenta la mayor fluctuación de uso: considerar ampliar oferta, optimizar señalización o promover alternativas.")
    print("3. Estudiar si la variabilidad está relacionada con eventos cercanos, actividad comercial o restricciones horarias.")

    return aparcamientos_analisis, zona_variabilidad


def query_with_pandas():
    """Demonstrate accessing and analyzing data directly with pandas."""
    print("\n=== Querying Access Zone with Pandas ===")

    # Load datasets from access zone
    print("Loading datasets from access-zone...")

    traffic_data = download_dataframe_from_minio(
        'access-zone',
        'trafico/cleaned_traffic.parquet',
        format='parquet'
    )

    parking_data = download_dataframe_from_minio(
        'access-zone',
        'parkings/cleaned_parking.parquet',
        format='parquet'
    )

    # 10 highest traffic and vehicle types records
    n = 10
    highest_traffic = traffic_data.sort_values(by='total_vehiculos', ascending=False).head(n)

    print("\nHorarios de mayor congestión en madrid y los tipos de vehículos predominantes en dichas franjas:")
    for _, row in highest_traffic.iterrows():
        vehicles = {
            'coches': row['coches'],
            'motos': row['motos'],
            'camiones': row['camiones'],
            'buses': row['buses']
        }
        highest_vehicle = max(vehicles, key=vehicles.get)
        
        print(f"    Fecha y hora: {row['fecha_hora']}")
        print(f"        Total de vehículos: {row['total_vehiculos']}")
        print(f"        Nivel de congestión: {row['nivel_congestion']}")
        print(f"        Vehículo predominante: {highest_vehicle} ({vehicles[highest_vehicle]})\n")

    return traffic_data, parking_data


def query_with_trino():
    """Demonstrate using SQL via Trino to query the data lake."""
    print("\n=== Querying Data Lake with Trino SQL ===")

    # 1_1: ¿Qué rutas de BiciMAD son más populares entre los usuarios? 
    # 1_2: ¿Cómo varían los patrones de uso entre usuarios abonados y ocasionales?
    query1_1 = """
        SELECT 
            estacion_origen,
            estacion_destino,
            COUNT(*) AS total_viajes
        FROM 
            viajes_bicimad
        GROUP BY 
            estacion_origen, estacion_destino
        ORDER BY 
            total_viajes DESC
        LIMIT 10;
        """

    query1_2 = """
        SELECT 
            tipo_usuario,
            COUNT(*) AS total_viajes,
            ROUND(AVG(duracion_segundos) / 60, 1) AS duracion_media_min,
            ROUND(AVG(distancia_km), 2) AS distancia_media_km,
            ROUND(AVG(calorias_estimadas), 1) AS calorias_medias,
            ROUND(AVG(co2_evitado_gramos), 1) AS co2_medio
        FROM 
            viajes_bicimad
        GROUP BY 
            tipo_usuario;
        """
    
    # 2: ¿Cómo se relaciona la densidad de población de los distritos con la presencia de infreastrctura de transporte público? 
    query2 = """
        SELECT 
            d.nombre AS distrito,
            d.densidad_poblacion,
            COUNT(e.id) AS numero_paradas
        FROM 
            distritos d
        LEFT JOIN 
            estaciones_transporte e ON d.distrito_id = e.distrito_id
        GROUP BY 
            d.nombre, d.densidad_poblacion
        ORDER BY 
            d.densidad_poblacion DESC
        """

    try:
        # Execute the queries using Trino
        execute_trino_query(query1_1)
        execute_trino_query(query1_2)
        execute_trino_query(query2)


    except Exception as e:
        print(f"Error with Trino demonstration: {e}")

    return None

def main():
    """Execute all query examples and generate insights."""
    print("Demonstrating various ways to query the multi-zone data lake...")

    # 1. Query with Pandas - direct access to the analytical datasets
    traffic_data, parking_data = query_with_pandas()

    # 2. Query with Trino SQL - for more complex analytical queries
    query_with_trino()

    # 3. Generate business insights from the data
    insights = generate_insights(parking_data)
    
if __name__ == "__main__":
    main()
