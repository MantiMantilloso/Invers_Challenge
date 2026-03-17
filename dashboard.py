import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de página
st.set_page_config(page_title="NYC TLC Dashboard", layout="wide")
st.title("🚕 NYC Yellow Taxi - Analytics")

# Conexión a BD
@st.cache_resource
def get_engine():
    DB_USER = os.getenv('POSTGRES_USER', 'tlc_user')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'tlc_password')
    DB_DB = os.getenv('POSTGRES_DB', 'tlc_database')
    DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    return create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_DB}')

engine = get_engine()

month_options_query = """
    SELECT DISTINCT TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') AS month
    FROM fact_trips
    ORDER BY month
"""
month_options = pd.read_sql(month_options_query, engine)['month'].tolist()

st.sidebar.header("Filtros")
selected_months = st.sidebar.multiselect(
    "Meses (YYYY-MM)",
    options=month_options,
    default=month_options,
)

if not selected_months:
    st.warning("Selecciona al menos un mes para mostrar información.")
    st.stop()

safe_selected_months = [
    month for month in selected_months
    if len(month) == 7 and month[4] == '-' and month.replace('-', '').isdigit()
]

if len(safe_selected_months) != len(selected_months):
    st.error("El filtro de meses contiene un formato inválido. Usa YYYY-MM.")
    st.stop()

months_sql = ", ".join([f"'{month}'" for month in safe_selected_months])

period_query = """
    SELECT
        MIN(DATE(tpep_pickup_datetime)) AS min_date,
        MAX(DATE(tpep_pickup_datetime)) AS max_date,
        COUNT(*) AS total_trips
    FROM fact_trips
    WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
"""
period_query = period_query.format(months_sql=months_sql)
df_period = pd.read_sql(period_query, engine)
min_date = df_period.loc[0, 'min_date']
max_date = df_period.loc[0, 'max_date']
total_trips = int(df_period.loc[0, 'total_trips'])
st.caption(f"Periodo cargado: {min_date} a {max_date} | Viajes totales: {total_trips:,}")

# Q1: Volumen total de transacciones por día (estacionalidad dentro del mes)
st.subheader("Q1: Volumen de Viajes por Día (Estacionalidad)")
q1_query = """
    SELECT DATE(tpep_pickup_datetime) as date, COUNT(*) as trips 
    FROM fact_trips 
    WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
    GROUP BY DATE(tpep_pickup_datetime) 
    ORDER BY date
"""
q1_query = q1_query.format(months_sql=months_sql)
df_q1 = pd.read_sql(q1_query, engine)
fig_q1 = px.line(df_q1, x='date', y='trips', markers=True, title='Viajes diarios en el periodo cargado')
st.plotly_chart(fig_q1, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    # Q2: Top 10 zonas con mayor valor generado
    st.subheader("Q2: Top 10 Zonas de Recojo por Ingresos")
    q2_enriched_query = """
        SELECT
            ft.pulocationid as location_id,
            COALESCE(dtz.zone_name, 'Zona desconocida') as zone_name,
            COALESCE(dtz.borough, 'N/A') as borough,
            SUM(ft.total_amount) as total_revenue
        FROM fact_trips ft
        LEFT JOIN dim_taxi_zone dtz
            ON dtz.location_id = ft.pulocationid
        WHERE TO_CHAR(ft.tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
        GROUP BY ft.pulocationid, dtz.zone_name, dtz.borough
        ORDER BY total_revenue DESC
        LIMIT 10
    """
    q2_enriched_query = q2_enriched_query.format(months_sql=months_sql)

    try:
        df_q2 = pd.read_sql(q2_enriched_query, engine)
    except Exception:
        st.warning("No se encontró dim_taxi_zone. Ejecuta load_taxi_zones.py para enriquecer las zonas.")
        q2_fallback_query = """
            SELECT pulocationid as location_id, SUM(total_amount) as total_revenue
            FROM fact_trips
            WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
            GROUP BY pulocationid
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        q2_fallback_query = q2_fallback_query.format(months_sql=months_sql)
        df_q2 = pd.read_sql(q2_fallback_query, engine)
        df_q2['zone_name'] = 'Zona desconocida'
        df_q2['borough'] = 'N/A'

    df_q2['zone_label'] = df_q2['zone_name'] + ' (' + df_q2['location_id'].astype(str) + ')'
    fig_q2 = px.bar(
        df_q2,
        x='zone_label',
        y='total_revenue',
        color='borough',
        title='Ingresos Totales por Zona de Recojo'
    )
    st.plotly_chart(fig_q2, use_container_width=True)

with col2:
    # Q3 & Q4: KPIs de Tiempo e Incidencias
    st.subheader("Q3 & Q4: KPIs Clave")
    
    # Q3: Tiempo promedio de viaje
    q3_query = """
        SELECT AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60) as avg_minutes 
        FROM fact_trips
        WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
    """
    q3_query = q3_query.format(months_sql=months_sql)
    avg_time = pd.read_sql(q3_query, engine).iloc[0, 0]
    st.metric("Tiempo Promedio de Viaje (Minutos)", f"{avg_time:.2f} min")
    
    # Q4: Porcentaje de incidencias (Tarifa $0, Disputas, o Tarjeta sin propina)
    q4_query = """
        SELECT 
            (SUM(CASE WHEN payment_type_id = 4 OR fare_amount <= 0 OR (payment_type_id = 1 AND tip_amount = 0) THEN 1 ELSE 0 END) * 100.0) / COUNT(*) as incidence_rate
        FROM fact_trips
        WHERE TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
    """
    q4_query = q4_query.format(months_sql=months_sql)
    incidence_rate = pd.read_sql(q4_query, engine).iloc[0, 0]
    st.metric("Tasa de Incidencias (Disputas/Sin Propina con Tarjeta)", f"{incidence_rate:.2f}%")

# Q5 (Pregunta Libre): Velocidad vs Hora del Día
st.subheader("Q5: ¿Cómo afecta la hora del día al costo por milla?")
q5_query = """
    SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) as hour_of_day, 
           AVG(total_amount / NULLIF(trip_distance, 0)) as avg_cost_per_mile
    FROM fact_trips
    WHERE trip_distance > 0 AND total_amount > 0
      AND TO_CHAR(tpep_pickup_datetime, 'YYYY-MM') IN ({months_sql})
    GROUP BY hour_of_day
    ORDER BY hour_of_day
"""
q5_query = q5_query.format(months_sql=months_sql)
df_q5 = pd.read_sql(q5_query, engine)
fig_q5 = px.bar(df_q5, x='hour_of_day', y='avg_cost_per_mile', title='Costo Promedio por Milla según Hora del Día')
st.plotly_chart(fig_q5, use_container_width=True)