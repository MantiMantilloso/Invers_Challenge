import pandas as pd
from sqlalchemy import create_engine, text
import time
from io import StringIO
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def load_data_to_postgres(file_path: str, db_url: str):
    total_start = time.time()
    print(f"Leyendo datos limpios de {file_path}...")
    df = pd.read_parquet(file_path)
    print(f"Archivo leído. Registros: {len(df)}")
    
    # Crear conexión a la base de datos
    engine = create_engine(db_url)
    
    print("Conexión a base de datos establecida. Creando dimensiones...")
    
    # --- MODELADO: DIMENSIONES ---
    # 1. Dimensión de Tipo de Pago (Payment Type)
    # Según el diccionario de datos de TLC: 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip
    dim_payment = pd.DataFrame({
        'payment_type_id': [1, 2, 3, 4, 5, 6],
        'payment_description': ['Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip']
    })
    
    # 2. Dimensión de Código de Tarifa (Rate Code)
    # 1=Standard rate, 2=JFK, 3=Newark, 4=Nassau or Westchester, 5=Negotiated fare, 6=Group ride
    dim_ratecode = pd.DataFrame({
        'ratecode_id': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        'ratecode_description': ['Standard rate', 'JFK', 'Newark', 'Nassau/Westchester', 'Negotiated fare', 'Group ride']
    })
    
    # --- CARGA DE DIMENSIONES ---
    dim_payment.to_sql('dim_payment_type', con=engine, if_exists='replace', index=False)
    dim_ratecode.to_sql('dim_ratecode', con=engine, if_exists='replace', index=False)
    print("Tablas de dimensiones cargadas exitosamente.")

    # --- MODELADO: TABLA DE HECHOS (FACT TABLE) ---
    # Seleccionamos las columnas relevantes para la tabla de hechos
    fact_trips = df[[
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 
        'passenger_count', 'trip_distance', 'ratecodeid', 'pulocationid', 
        'dolocationid', 'payment_type', 'fare_amount', 'tip_amount', 
        'tolls_amount', 'total_amount', 'congestion_surcharge', 'airport_fee'
    ]].copy()
    
    # Renombrar algunas columnas para mantener consistencia con las dimensiones
    fact_trips.rename(columns={
        'ratecodeid': 'ratecode_id',
        'payment_type': 'payment_type_id'
    }, inplace=True)
    
    # --- CARGA DE HECHOS ---
    print(f"Iniciando carga de la tabla de hechos (fact_trips) con {len(fact_trips)} registros...")
    load_start = time.time()

    # Crear tabla vacía con esquema correcto (sin cargar datos todavía)
    fact_trips.head(0).to_sql(name='fact_trips', con=engine, if_exists='replace', index=False)

    # COPY es mucho más rápido que INSERT para millones de filas.
    chunksize = 250000
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for start in range(0, len(fact_trips), chunksize):
            end = min(start + chunksize, len(fact_trips))
            chunk = fact_trips.iloc[start:end].copy()

            # Serializar datetime para COPY CSV
            for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                chunk[col] = chunk[col].dt.strftime('%Y-%m-%d %H:%M:%S')

            buffer = StringIO()
            chunk.to_csv(buffer, index=False, header=False, na_rep='')
            buffer.seek(0)

            cursor.copy_expert(
                """
                COPY fact_trips (
                    vendorid,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    ratecode_id,
                    pulocationid,
                    dolocationid,
                    payment_type_id,
                    fare_amount,
                    tip_amount,
                    tolls_amount,
                    total_amount,
                    congestion_surcharge,
                    airport_fee
                )
                FROM STDIN WITH (FORMAT CSV)
                """,
                buffer,
            )

            raw_conn.commit()
            print(f"Chunk cargado: {end}/{len(fact_trips)}")
    finally:
        raw_conn.close()
    
    # Crear índices para mejorar rendimiento de queries
    with engine.connect() as conn:
        print("Creando índices...")
        conn.execute(text("CREATE INDEX idx_payment_type ON fact_trips(payment_type_id)"))
        conn.execute(text("CREATE INDEX idx_ratecode ON fact_trips(ratecode_id)"))
        conn.execute(text("CREATE INDEX idx_pickup_time ON fact_trips(tpep_pickup_datetime)"))
        conn.execute(text("CREATE INDEX idx_pickup_location ON fact_trips(pulocationid)"))
        conn.commit()
    
    end_time = time.time()
    load_elapsed = round(end_time - load_start, 2)
    total_elapsed = round(end_time - total_start, 2)
    records_per_sec = round(len(fact_trips) / max((end_time - load_start), 1), 0)
    print(f"Carga de hechos finalizada en {load_elapsed} segundos ({records_per_sec} registros/seg).")
    print(f"Proceso completo finalizado en {total_elapsed} segundos.")

if __name__ == "__main__":
    # Ruta del archivo limpio que generamos en el Día 1
    CLEAN_FILE = 'data/clean/yellow_tripdata_2025_01_clean.parquet'
    
    # Credenciales desde .env (mismo que docker-compose.yml)
    # Formato: postgresql://usuario:password@host:puerto/base_de_datos
    DB_USER = os.getenv('POSTGRES_USER', 'tlc_user')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'tlc_password')
    DB_NAME = os.getenv('POSTGRES_DB', 'tlc_database')
    DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    DB_PORT = os.getenv('POSTGRES_PORT', '5432')
    
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    
    load_data_to_postgres(CLEAN_FILE, DB_URL)