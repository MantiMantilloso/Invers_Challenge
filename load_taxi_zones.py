import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

load_dotenv()


def load_taxi_zones_to_postgres(db_url: str, source_url: str = TAXI_ZONE_LOOKUP_URL) -> None:
    """Descarga el catálogo de zonas TLC y lo guarda como dimensión en PostgreSQL."""
    print(f"Descargando lookup de zonas desde: {source_url}")
    zone_df = pd.read_csv(source_url)
    zone_df.columns = [c.strip().lower() for c in zone_df.columns]

    dim_taxi_zone = zone_df.rename(
        columns={
            "locationid": "location_id",
            "zone": "zone_name",
            "service_zone": "service_zone",
        }
    )[["location_id", "borough", "zone_name", "service_zone"]]

    engine = create_engine(db_url)
    dim_taxi_zone.to_sql("dim_taxi_zone", con=engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX idx_dim_taxi_zone_location ON dim_taxi_zone(location_id)"))
        conn.commit()

    print(f"Dimensión dim_taxi_zone cargada exitosamente. Registros: {len(dim_taxi_zone)}")


if __name__ == "__main__":
    DB_USER = os.getenv("POSTGRES_USER", "tlc_user")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "tlc_password")
    DB_NAME = os.getenv("POSTGRES_DB", "tlc_database")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")

    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    load_taxi_zones_to_postgres(DB_URL)
