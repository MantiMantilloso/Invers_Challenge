import pandas as pd
import os

def clean_tlc_data(file_url: str, output_path: str):
    print(f"Descargando y leyendo datos de: {file_url}...")
    df = pd.read_parquet(file_url)
    
    # 1. Estandarización de nombres de columnas
    # Convertimos todo a minúsculas para evitar problemas de 'Airport_fee' vs 'airport_fee'
    df.columns = [col.lower() for col in df.columns]
    
    print(f"Registros iniciales: {len(df)}")
    
    # 2. Manejo de valores nulos
    # Asumimos que si no hay 'passenger_count', es un error de registro.
    df = df.dropna(subset=['passenger_count', 'tpep_pickup_datetime'])
    
    # Llenar nulos en tarifas condicionales con 0
    cols_to_fill_zero = ['congestion_surcharge', 'airport_fee', 'cbd_congestion_fee']
    for col in cols_to_fill_zero:
        if col in df.columns:
            df[col] = df[col].fillna(0)
            
    # 3. Filtros de anomalías de negocio
    # Un viaje no puede tener distancia 0 o negativa
    df = df[df['trip_distance'] > 0]
    
    # Las tarifas y el total no pueden ser negativos
    df = df[df['fare_amount'] > 0]
    df = df[df['total_amount'] > 0]
    
    # Eliminar viajes con duraciones ilógicas (ej. menores a 1 minuto o mayores a 10 horas)
    duracion = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    duracion_minutos = duracion.dt.total_seconds() / 60
    df = df[(duracion_minutos >= 1) & (duracion_minutos <= 600)]
    
    print(f"Registros después de limpieza: {len(df)}")
    
    # 4. Guardar dataset limpio
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Datos limpios guardados en: {output_path}\n")

if __name__ == "__main__":
    # Usaremos Yellow Taxi de Enero 2025 como dataset principal
    URL_2025 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'
    OUTPUT_FILE = 'data/clean/yellow_tripdata_2025_01_clean.parquet'
    
    clean_tlc_data(URL_2025, OUTPUT_FILE)