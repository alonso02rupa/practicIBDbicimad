"""
This script demonstrates querying the data in the access zone using different methods:
1. Direct Pandas querying from the access-zone for traffic data.
"""

from utils import (
    download_dataframe_from_minio
)
import pandas as pd

def query_with_pandas():
    """Demonstrate accessing and analyzing traffic data directly with pandas."""
    print("\n=== Querying Access Zone with Pandas ===")

    # Load traffic dataset from access zone
    print("Loading traffic dataset from access-zone...")

    traffic_data = download_dataframe_from_minio(
        'access-zone',
        'trafico/cleaned_traffic.parquet',
        format='parquet'
    )

    # Agrupar los datos de tráfico por hora y sumar las diferentes categorías de vehículos
    traffic_data = traffic_data.groupby('hora').agg({
        'coches': 'sum',
        'motos': 'sum',
        'camiones': 'sum',
        'buses': 'sum',
        'total_vehiculos': 'sum',
        'nivel_congestion': lambda x: x.mode()[0] if not x.mode().empty else None
    }).reset_index()

    # 10 registros con mayor tráfico y tipos de vehículos predominantes
    n = 10
    highest_traffic = traffic_data.sort_values(by='total_vehiculos', ascending=False).head(n)

    print("\nHorarios de mayor congestión en Madrid y los tipos de vehículos predominantes en dichas franjas:")
    for _, row in highest_traffic.iterrows():
        vehicles = {
            'coches': row['coches'],
            'motos': row['motos'],
            'camiones': row['camiones'],
            'buses': row['buses']
        }
        highest_vehicle = max(vehicles, key=vehicles.get)
        print(f"    Hora: {row['hora']}")
        print(f"        Total de vehículos: {row['total_vehiculos']}")
        print(f"        Nivel de congestión: {row['nivel_congestion']}")
        print(f"        Vehículo predominante: {highest_vehicle} ({vehicles[highest_vehicle]})\n")

    return traffic_data


def main():
    """Execute query examples focused on traffic data."""
    print("Demonstrating querying traffic data from the access zone...")

    # 1. Query with Pandas - direct access to the traffic data
    traffic_data = query_with_pandas()
    print(traffic_data)
    
if __name__ == "__main__":
    main()
