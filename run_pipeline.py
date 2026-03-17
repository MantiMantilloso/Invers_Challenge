import os
import time
import argparse
from clean_data import clean_tlc_data
from load_data import load_data_to_postgres, create_fact_indexes
from load_taxi_zones import load_taxi_zones_to_postgres
from dotenv import load_dotenv

def main(year: int, retries: int = 3, retry_wait_seconds: int = 5):
    print(f"=== INICIANDO PIPELINE NYC TLC ({year}) ===")
    
    # Configuración
    load_dotenv()
    monthly_sources = [
        (
            f"{year}-{month:02d}",
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet",
        )
        for month in range(1, 13)
    ]
    os.makedirs('data/clean', exist_ok=True)
    
    DB_USER = os.getenv('POSTGRES_USER', 'tlc_user')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'tlc_password')
    DB_NAME = os.getenv('POSTGRES_DB', 'tlc_database')
    DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    DB_PORT = os.getenv('POSTGRES_PORT', '5432')
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    
    # Fase 1, 2 y 3: limpiar y cargar por mes (sin acumular todo el año en memoria)
    print("\n--- FASE 1, 2 & 3: Limpieza y Carga Mensual ---")
    first_loaded = True
    loaded_months = []

    for month, source_url in monthly_sources:
        month_output_path = f"data/clean/yellow_tripdata_{month.replace('-', '_')}_clean.parquet"
        print(f"\nProcesando mes {month}...")

        cleaned_ok = False
        for attempt in range(1, retries + 1):
            try:
                clean_tlc_data(source_url, month_output_path, expected_month=month)
                cleaned_ok = True
                break
            except Exception as exc:
                print(f"Intento {attempt}/{retries} falló para {month}: {exc}")
                if attempt < retries:
                    print(f"Reintentando en {retry_wait_seconds} segundos...")
                    time.sleep(retry_wait_seconds)

        if not cleaned_ok:
            print(f"No se pudo limpiar {month} después de {retries} intentos. Continuando...")
            continue

        try:
            mode = 'replace' if first_loaded else 'append'
            load_data_to_postgres(
                month_output_path,
                DB_URL,
                if_exists=mode,
                create_dimensions=first_loaded,
                create_indexes=False,
            )
            first_loaded = False
            loaded_months.append(month)
        except Exception as exc:
            print(f"No se pudo cargar {month} en PostgreSQL: {exc}")
            print("Continuando con el siguiente mes...")

    if not loaded_months:
        raise RuntimeError("No se pudo cargar ningún mes en PostgreSQL.")

    # Crear índices al final mejora el rendimiento total de carga
    from sqlalchemy import create_engine
    engine = create_engine(DB_URL)
    create_fact_indexes(engine)

    # Fase 4: Carga de Catálogo de Zonas TLC
    print("\n--- FASE 4: Carga de Dimensión de Zonas ---")
    load_taxi_zones_to_postgres(DB_URL)
    
    print(f"\n=== PIPELINE COMPLETADO EXITOSAMENTE ({year}) ===")
    print(f"Meses cargados: {len(loaded_months)}/12 -> {', '.join(loaded_months)}")
    print("Para ver los resultados, ejecuta: streamlit run dashboard.py")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline NYC TLC dinámico por año")
    parser.add_argument("--year", type=int, default=2025, help="Año a procesar (ej. 2025)")
    parser.add_argument("--retries", type=int, default=3, help="Reintentos por mes ante fallos de descarga")
    parser.add_argument("--retry-wait", type=int, default=5, help="Segundos de espera entre reintentos")
    args = parser.parse_args()

    main(args.year, retries=args.retries, retry_wait_seconds=args.retry_wait)
