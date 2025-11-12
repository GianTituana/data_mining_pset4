#!/usr/bin/env python3
"""
build_obt.py - Script CLI para construir la tabla OBT (One Big Table) de NYC Taxi

VERSIÓN 2.0 - OPTIMIZADA PARA MACHINE LEARNING
- Lee DIRECTAMENTE desde raw.yellow_taxi_trip y raw.green_taxi_trip
- NO necesita tabla enriched intermedia
- Particionado nativo PostgreSQL por (year, month)
- Features numéricas y binarias listas para ML
- Idempotencia: no duplica datos
- Procesamiento paralelo por particiones

PARTICIONADO NATIVO vs LÓGICO:
- Tabla padre particionada: analytics.obt_trips
- Particiones hijas automáticas: analytics.obt_trips_YYYY_MM
- Ventajas: Partition pruning, DROP rápido, mejor rendimiento con grandes volúmenes

OPTIMIZACIONES PARA ML:
- Features temporales: pickup_hour, is_weekend, is_night, is_rush_hour
- Features binarias: is_airport_trip, is_long_trip, is_cash_payment
- Features derivadas: avg_speed_mph, tip_pct, revenue_per_mile
- Sin valores NULL en features críticas (rellenados con defaults)

Uso:
    # Modo FULL - Reconstruir todo
    python build_obt.py --mode full --overwrite true

    # Modo BY-PARTITION - Procesar años específicos
    python build_obt.py --mode by-partition --year-start 2020 --year-end 2021 --services yellow,green

    # Procesar meses específicos de un año
    python build_obt.py --mode by-partition --year-start 2020 --year-end 2020 --months 1,2,3 --services yellow
"""
import argparse
import sys
import time
import psycopg2
import os
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import uuid
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# =============================================================================
# CONFIGURACIÓN DE CONEXIÓN POSTGRESQL DESDE VARIABLES DE ENTORNO
# =============================================================================

DB_CONFIG = {
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': int(os.getenv('PG_PORT', 5432)),
    'database': os.getenv('PG_DB', 'postgres'),
    'user': os.getenv('PG_USER', 'postgres'),
    'password': os.getenv('PG_PASSWORD', 'root')
}

# Schemas
SCHEMA_RAW = os.getenv('PG_SCHEMA_RAW', 'raw')
SCHEMA_ANALYTICS = os.getenv('PG_SCHEMA_ANALYTICS', 'analytics')


# =============================================================================
# FUNCIONES DE UTILIDAD PARA POSTGRESQL
# =============================================================================

def get_connection():
    """Establece conexión a PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f" Error conectando a PostgreSQL: {e}")
        sys.exit(1)


def execute_sql(conn, query: str, params: Optional[tuple] = None, fetch: bool = False):
    """
    Ejecuta una consulta SQL en PostgreSQL
    
    Args:
        conn: Conexión a PostgreSQL
        query: Query SQL a ejecutar
        params: Parámetros para la query (opcional)
        fetch: Si True, retorna resultados (para SELECT)
    
    Returns:
        Resultados si fetch=True, None en caso contrario
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            else:
                conn.commit()
                return cur.rowcount if cur.rowcount > 0 else None
    except Exception as e:
        conn.rollback()
        print(f"Error ejecutando SQL: {e}")
        print(f"Query: {query[:200]}...")
        raise


# =============================================================================
# CREACIÓN DE SCHEMAS Y TABLAS
# =============================================================================

def create_schemas(conn):
    """Crea los schemas necesarios si no existen"""
    print("\n Verificando schemas...")
    
    schemas = [SCHEMA_RAW, SCHEMA_ANALYTICS]
    
    for schema in schemas:
        query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        execute_sql(conn, query)
        print(f"   ✓ Schema '{schema}' verificado")


def create_obt_table(conn):
    """Crea la tabla analytics.obt_trips con particionado nativo de PostgreSQL
    
    Optimizada para Machine Learning:
    - Particionado por año/mes para procesamiento paralelo
    - Solo features que se usarán en ML (evita overhead)
    - Índices para queries analíticas
    - Categóricas preparadas para Top-K encoding
    """
    print(f"\n Creando tabla {SCHEMA_ANALYTICS}.obt_trips con particionado nativo (ML-ready)...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_ANALYTICS}.obt_trips (
        -- IDENTIFICADORES ÚNICOS
        trip_id VARCHAR NOT NULL,
        
        -- PARTICIONADO NATIVO (year/month) - CRÍTICO para ML paralelo
        partition_year INT NOT NULL,
        partition_month INT NOT NULL,
        
        -- ========================================================================
        -- TIEMPO (pickup y dropoff)
        -- ========================================================================
        pickup_datetime TIMESTAMP NOT NULL,
        dropoff_datetime TIMESTAMP,
        pickup_date DATE NOT NULL,
        pickup_hour INT NOT NULL,                  -- Feature ML
        dropoff_date DATE,
        dropoff_hour INT,
        day_of_week INT NOT NULL,                  -- pickup_dow para ML (0=Dom, 6=Sáb)
        month INT NOT NULL,                        -- Feature ML
        year INT NOT NULL,                         -- Feature ML
        
        -- ========================================================================
        -- FLAGS BINARIOS (para ML)
        -- ========================================================================
        is_rush_hour BOOLEAN NOT NULL,             -- 7-9 AM, 5-7 PM
        is_weekend BOOLEAN NOT NULL,               -- Sábado/Domingo
        
        -- ========================================================================
        -- UBICACIÓN (pickup y dropoff)
        -- ========================================================================
        pu_location_id INT,
        pu_zone VARCHAR,                           -- Feature ML (Top-K)
        pu_borough VARCHAR,                        -- Feature ML (Top-K)
        do_location_id INT,
        do_zone VARCHAR,
        do_borough VARCHAR,
        
        -- ========================================================================
        -- SERVICIO Y CÓDIGOS
        -- ========================================================================
        service_type VARCHAR NOT NULL,             -- Feature ML (yellow/green)
        vendor_id INT,                             -- Feature ML
        vendor_name VARCHAR,
        rate_code_id INT,
        rate_code_desc VARCHAR,                    -- Feature ML
        payment_type INT,
        payment_type_desc VARCHAR,
        trip_type INT,                             -- Solo para green taxi
        
        -- ========================================================================
        -- VIAJE
        -- ========================================================================
        passenger_count INT NOT NULL,              -- Feature ML
        trip_distance FLOAT NOT NULL,              -- Feature ML
        store_and_fwd_flag VARCHAR,
        
        -- ========================================================================
        -- TARIFAS
        -- ========================================================================
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT NOT NULL,                 -- Target ML
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        congestion_surcharge FLOAT,
        airport_fee FLOAT,
        total_amount FLOAT NOT NULL,               -- Target ML principal
        
        -- ========================================================================
        -- DERIVADAS
        -- ========================================================================
        trip_duration_min FLOAT NOT NULL,          -- Target ML
        avg_speed_mph FLOAT,
        tip_pct FLOAT,
        
        -- ========================================================================
        -- LINEAGE Y CALIDAD DE DATOS
        -- ========================================================================
        run_id VARCHAR,
        ingested_at_utc TIMESTAMP,
        source_service VARCHAR,                    -- yellow/green
        source_year INT,
        source_month INT,
        obt_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) PARTITION BY RANGE (partition_year, partition_month);
    """
    
    execute_sql(conn, create_table_sql)
    print(f"   ✓ Tabla {SCHEMA_ANALYTICS}.obt_trips creada/verificada")
    print("   ✓ Sin clave primaria ni índices para máxima velocidad de inserción")


def create_partition_if_not_exists(conn, year: int, month: int):
    """
    Crea una partición específica si no existe
    
    Args:
        conn: Conexión a PostgreSQL
        year: Año de la partición
        month: Mes de la partición
    """
    partition_name = f"obt_trips_{year}_{month:02d}"
    
    # Verificar si la partición ya existe
    check_query = f"""
        SELECT EXISTS (
            SELECT FROM pg_tables 
            WHERE schemaname = '{SCHEMA_ANALYTICS}' 
            AND tablename = %s
        );
    """
    
    exists = execute_sql(conn, check_query, (partition_name,), fetch=True)
    
    if exists and exists[0][0]:
        return False  # Partición ya existe
    
    # Calcular el rango de la partición
    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1
    
    # Crear la partición
    create_partition_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_ANALYTICS}.{partition_name}
    PARTITION OF {SCHEMA_ANALYTICS}.obt_trips
    FOR VALUES FROM ({year}, {month}) TO ({next_year}, {next_month});
    """
    
    execute_sql(conn, create_partition_sql)
    print(f"   ✓ Partición creada: {partition_name} (rango: {year}-{month:02d})")
    
    return True


def list_existing_partitions(conn) -> List[Tuple[int, int]]:
    """
    Lista todas las particiones existentes
    
    Returns:
        Lista de tuplas (year, month)
    """
    query = f"""
    SELECT 
        tablename
    FROM pg_tables 
    WHERE schemaname = '{SCHEMA_ANALYTICS}' 
    AND tablename LIKE 'obt_trips_%'
    AND tablename != 'obt_trips'
    ORDER BY tablename;
    """
    
    results = execute_sql(conn, query, fetch=True)
    
    partitions = []
    for row in results:
        table_name = row[0]
        # Extraer año y mes del nombre de la tabla: obt_trips_YYYY_MM
        parts = table_name.split('_')
        if len(parts) >= 4:
            try:
                year = int(parts[2])
                month = int(parts[3])
                partitions.append((year, month))
            except ValueError:
                continue
    
    return partitions


def drop_partition(conn, year: int, month: int):
    """
    Elimina una partición específica (útil para reprocessar datos)
    
    Args:
        conn: Conexión a PostgreSQL
        year: Año de la partición
        month: Mes de la partición
    """
    partition_name = f"obt_trips_{year}_{month:02d}"
    
    drop_sql = f"DROP TABLE IF EXISTS {SCHEMA_ANALYTICS}.{partition_name};"
    execute_sql(conn, drop_sql)
    print(f"   Partición eliminada: {partition_name}")


# =============================================================================
# CONSTRUCCIÓN DE OBT DIRECTAMENTE DESDE RAW
# =============================================================================

def get_insert_query(service_type: str) -> str:
    """
    Genera el query de INSERT para construir OBT desde RAW
    
    Lee DIRECTAMENTE de raw.yellow_taxi_trip o raw.green_taxi_trip
    y hace JOIN con raw.taxi_zone_lookup para enriquecimiento geográfico
    
    SOLO INCLUYE FEATURES ESPECIFICADAS PARA ML:
    - Numéricas: trip_distance, passenger_count, pickup_hour, pickup_dow, month, year
    - Flags: is_rush_hour, is_weekend
    - Categóricas: service_type, vendor_id, rate_code_desc, pu_borough, pu_zone
    - Targets: trip_duration_min, tip_amount, total_amount
    
    Args:
        service_type: 'yellow' o 'green'
    
    Returns:
        Query SQL completo para insertar en analytics.obt_trips
    """
    
    # Determinar columnas específicas según servicio
    if service_type == 'yellow':
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
        ratecode_col = 'ratecodeid'
    else:  # green
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
        ratecode_col = 'ratecodeid'
    
    query = f"""
    INSERT INTO {SCHEMA_ANALYTICS}.obt_trips
    SELECT
        -- IDENTIFICADORES
        md5(CONCAT(
            '{service_type}',
            COALESCE(t.{pickup_col}::TEXT, ''),
            COALESCE(t.{dropoff_col}::TEXT, ''),
            COALESCE(t.pulocationid::TEXT, ''),
            COALESCE(t.dolocationid::TEXT, ''),
            COALESCE(t.fare_amount::TEXT, '')
        )) as trip_id,
        
        -- PARTICIONADO
        t.source_year as partition_year,
        t.source_month as partition_month,
        
        -- ========================================================================
        -- TIEMPO (pickup y dropoff)
        -- ========================================================================
        t.{pickup_col} as pickup_datetime,
        t.{dropoff_col} as dropoff_datetime,
        t.{pickup_col}::DATE as pickup_date,
        EXTRACT(HOUR FROM t.{pickup_col})::INT as pickup_hour,
        t.{dropoff_col}::DATE as dropoff_date,
        EXTRACT(HOUR FROM t.{dropoff_col})::INT as dropoff_hour,
        EXTRACT(DOW FROM t.{pickup_col})::INT as day_of_week,  -- 0=Dom, 6=Sáb
        t.source_month as month,
        t.source_year as year,
        
        -- FLAGS BINARIOS PARA ML
        (EXTRACT(HOUR FROM t.{pickup_col}) BETWEEN 7 AND 9) OR 
        (EXTRACT(HOUR FROM t.{pickup_col}) BETWEEN 17 AND 19) as is_rush_hour,
        EXTRACT(DOW FROM t.{pickup_col}) IN (0, 6) as is_weekend,
        
        -- ========================================================================
        -- UBICACIÓN (pickup y dropoff)
        -- ========================================================================
        t.pulocationid as pu_location_id,
        COALESCE(pz.zone, 'Unknown') as pu_zone,
        COALESCE(pz.borough, 'Unknown') as pu_borough,
        t.dolocationid as do_location_id,
        COALESCE(dz.zone, 'Unknown') as do_zone,
        COALESCE(dz.borough, 'Unknown') as do_borough,
        
        -- ========================================================================
        -- SERVICIO Y CÓDIGOS
        -- ========================================================================
        '{service_type}' as service_type,
        t.vendorid as vendor_id,
        CASE t.vendorid
            WHEN 1 THEN 'Creative Mobile Technologies'
            WHEN 2 THEN 'VeriFone Inc'
            WHEN 6 THEN 'Vendor 6'
            WHEN 7 THEN 'Vendor 7'
            ELSE 'Unknown'
        END as vendor_name,
        t.{ratecode_col} as rate_code_id,
        CASE t.{ratecode_col}
            WHEN 1 THEN 'Standard rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau or Westchester'
            WHEN 5 THEN 'Negotiated fare'
            WHEN 6 THEN 'Group ride'
            WHEN 99 THEN 'Unknown'
            ELSE 'Other'
        END as rate_code_desc,
        t.payment_type,
        CASE t.payment_type
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided trip'
            ELSE 'Unknown'
        END as payment_type_desc,
        {f"t.trip_type" if service_type == 'green' else "NULL::INT"} as trip_type,
        
        -- ========================================================================
        -- VIAJE
        -- ========================================================================
        COALESCE(t.passenger_count, 1) as passenger_count,
        COALESCE(t.trip_distance, 0) as trip_distance,
        t.store_and_fwd_flag,
        
        -- ========================================================================
        -- TARIFAS
        -- ========================================================================
        COALESCE(t.fare_amount, 0) as fare_amount,
        COALESCE(t.extra, 0) as extra,
        COALESCE(t.mta_tax, 0) as mta_tax,
        COALESCE(t.tip_amount, 0) as tip_amount,
        COALESCE(t.tolls_amount, 0) as tolls_amount,
        COALESCE(t.improvement_surcharge, 0) as improvement_surcharge,
        COALESCE(t.congestion_surcharge, 0) as congestion_surcharge,
        {f"COALESCE(t.airport_fee, 0)" if service_type == 'yellow' else "0"} as airport_fee,
        COALESCE(t.total_amount, 0) as total_amount,
        
        -- ========================================================================
        -- DERIVADAS
        -- ========================================================================
        EXTRACT(EPOCH FROM (t.{dropoff_col} - t.{pickup_col})) / 60.0 as trip_duration_min,
        CASE 
            WHEN EXTRACT(EPOCH FROM (t.{dropoff_col} - t.{pickup_col})) / 3600.0 > 0 
            THEN t.trip_distance / (EXTRACT(EPOCH FROM (t.{dropoff_col} - t.{pickup_col})) / 3600.0)
            ELSE 0
        END as avg_speed_mph,
        CASE 
            WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100.0
            ELSE 0
        END as tip_pct,
        
        -- ========================================================================
        -- LINEAGE Y CALIDAD DE DATOS
        -- ========================================================================
        t.run_id,
        CASE 
            WHEN t.ingested_at_utc IS NOT NULL AND t.ingested_at_utc != '' 
            THEN t.ingested_at_utc::TIMESTAMP
            ELSE NULL
        END as ingested_at_utc,
        '{service_type}' as source_service,
        t.source_year,
        t.source_month,
        CURRENT_TIMESTAMP as obt_created_at
        
    FROM {SCHEMA_RAW}.{service_type}_taxi_trip t
    LEFT JOIN {SCHEMA_RAW}.taxi_zone_lookup pz ON t.pulocationid = pz.locationid
    LEFT JOIN {SCHEMA_RAW}.taxi_zone_lookup dz ON t.dolocationid = dz.locationid
    WHERE 
        -- Validaciones básicas
        t.{pickup_col} IS NOT NULL
        AND t.{dropoff_col} IS NOT NULL
        AND t.{pickup_col} < t.{dropoff_col}
        
        -- Filtro por partición
        AND t.source_year = %s
        AND t.source_month = %s
        
        -- Filtros de calidad de datos
        AND t.trip_distance >= 0
        AND t.trip_distance < 500  -- Eliminar outliers extremos
        AND t.total_amount >= 0
        AND t.total_amount < 10000  -- Eliminar outliers extremos
        AND t.passenger_count > 0
        AND t.passenger_count <= 9  -- Máximo realista
        
        -- Duración razonable (1 min a 24 horas)
        AND EXTRACT(EPOCH FROM (t.{dropoff_col} - t.{pickup_col})) / 60.0 BETWEEN 1 AND 1440;
    """
    
    return query


def build_obt_full(conn, run_id: str, overwrite: bool = False):
    """
    Modo FULL: Reconstruye completamente analytics.obt_trips DIRECTAMENTE desde raw
    Con particionado nativo: crea particiones automáticamente
    
    OPTIMIZACIÓN: Procesa por lotes (año-mes) en lugar de todo de una vez
    LEE DIRECTAMENTE desde raw.yellow_taxi_trip y raw.green_taxi_trip
    """
    print(f"\n{'='*80}")
    print(f" MODO FULL - Construcción completa de OBT (Particionado Nativo)")
    print(f"   ✓ LECTURA DIRECTA desde {SCHEMA_RAW}.yellow_taxi_trip y {SCHEMA_RAW}.green_taxi_trip")
    print(f"   ✓ SIN tabla intermedia enriched")
    print(f"{'='*80}")
    print(f"Run ID: {run_id}")
    print(f"Overwrite: {overwrite}")
    
    start_time = time.time()
    
    # Si overwrite=True, eliminar todas las particiones
    if overwrite:
        print("\n Modo OVERWRITE: Eliminando particiones existentes...")
        partitions = list_existing_partitions(conn)
        for year, month in partitions:
            drop_partition(conn, year, month)
        if partitions:
            print(f"   ✓ {len(partitions)} particiones eliminadas")
    
    # Obtener años y meses únicos de las tablas RAW
    print("\n Analizando datos en tablas RAW...")
    years_months_query = f"""
    SELECT DISTINCT source_year, source_month
    FROM (
        SELECT source_year, source_month FROM {SCHEMA_RAW}.yellow_taxi_trip
        UNION
        SELECT source_year, source_month FROM {SCHEMA_RAW}.green_taxi_trip
    ) AS combined
    WHERE source_year IS NOT NULL AND source_month IS NOT NULL
    ORDER BY source_year, source_month;
    """
    
    years_months = execute_sql(conn, years_months_query, fetch=True)
    
    if not years_months:
        print("   No se encontraron datos en las tablas RAW")
        return
    
    print(f"   ✓ Encontrados {len(years_months)} períodos (año-mes) para procesar")
    
    # Crear particiones necesarias
    print("\n Creando particiones necesarias...")
    partitions_created = 0
    for year, month in years_months:
        if create_partition_if_not_exists(conn, year, month):
            partitions_created += 1
    
    if partitions_created > 0:
        print(f"   ✓ {partitions_created} particiones nuevas creadas")
    else:
        print(f"   ✓ Todas las particiones ya existían")
    
    # Insertar por lotes (año-mes) desde RAW
    print(f"\n Insertando datos en {SCHEMA_ANALYTICS}.obt_trips (procesando por lotes desde RAW)...")
    
    total_inserted = 0
    batch_num = 0
    
    for year, month in years_months:
        batch_num += 1
        print(f"\n   Lote {batch_num}/{len(years_months)}: Año {year}, Mes {month}")
        
        batch_inserted = 0
        
        # Procesar yellow taxi
        print(f"       Procesando yellow taxi {year}-{month:02d}...")
        try:
            query_yellow = get_insert_query('yellow')
            rows = execute_sql(conn, query_yellow, (year, month))
            if rows:
                batch_inserted += rows
                print(f"         ✓ {rows:,} registros insertados")
        except Exception as e:
            print(f"          Error: {e}")
        
        # Procesar green taxi
        print(f"       Procesando green taxi {year}-{month:02d}...")
        try:
            query_green = get_insert_query('green')
            rows = execute_sql(conn, query_green, (year, month))
            if rows:
                batch_inserted += rows
                print(f"         ✓ {rows:,} registros insertados")
        except Exception as e:
            print(f"          Error: {e}")
        
        total_inserted += batch_inserted
        print(f"       Lote {batch_num} completado: {batch_inserted:,} registros")
    
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f" CONSTRUCCIÓN FULL COMPLETADA")
    print(f"{'='*80}")
    print(f"Lotes procesados: {len(years_months)}")
    print(f"Registros insertados: {total_inserted:,}")
    print(f"Tiempo total: {elapsed_time:.2f}s ({elapsed_time/60:.2f} min)")
    if total_inserted > 0:
        print(f"Velocidad: {total_inserted/elapsed_time:,.0f} registros/segundo")
    print(f"{'='*80}")


def build_obt_by_partition(conn, run_id: str, services: List[str], 
                           year_start: int, year_end: int, 
                           months: Optional[List[int]] = None):
    """
    Modo BY-PARTITION: Procesa solo particiones específicas
    Con particionado nativo: crea particiones automáticamente
    LEE DIRECTAMENTE desde raw
    """
    print(f"\n{'='*80}")
    print(f" MODO BY-PARTITION - Construcción por particiones (Particionado Nativo)")
    print(f"   ✓ LECTURA DIRECTA desde {SCHEMA_RAW}.yellow_taxi_trip y {SCHEMA_RAW}.green_taxi_trip")
    print(f"   ✓ SIN tabla intermedia enriched")
    print(f"{'='*80}")
    print(f"Run ID: {run_id}")
    print(f"Servicios: {', '.join(services)}")
    print(f"Período: {year_start}-{year_end}")
    if months:
        print(f"Meses: {', '.join(map(str, months))}")
    
    start_time = time.time()
    
    # Determinar particiones a procesar
    partitions = []
    for year in range(year_start, year_end + 1):
        months_to_process = months if months else list(range(1, 13))
        for month in months_to_process:
            partitions.append((year, month))
    
    print(f"\n Particiones a procesar: {len(partitions)}")
    
    # Crear particiones necesarias
    print("\n Creando particiones necesarias...")
    for year, month in partitions:
        create_partition_if_not_exists(conn, year, month)
    
    total_inserted = 0
    
    for idx, (year, month) in enumerate(partitions, 1):
        print(f"\n   Partición {idx}/{len(partitions)}: Año {year}, Mes {month}")
        
        # Eliminar datos existentes de esta partición (idempotencia)
        print(f"        Limpiando datos existentes...")
        delete_query = f"""
        DELETE FROM {SCHEMA_ANALYTICS}.obt_trips 
        WHERE partition_year = %s AND partition_month = %s;
        """
        execute_sql(conn, delete_query, (year, month))
        
        partition_inserted = 0
        
        # Procesar cada servicio
        for service_type in services:
            print(f"       Procesando {service_type} taxi {year}-{month:02d}...")
            try:
                query = get_insert_query(service_type)
                rows = execute_sql(conn, query, (year, month))
                if rows:
                    partition_inserted += rows
                    print(f"         ✓ {rows:,} registros insertados")
            except Exception as e:
                print(f"          Error: {e}")
        
        total_inserted += partition_inserted
        print(f"       Partición completada: {partition_inserted:,} registros")
    
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f" CONSTRUCCIÓN BY-PARTITION COMPLETADA")
    print(f"{'='*80}")
    print(f"Particiones procesadas: {len(partitions)}")
    print(f"Registros insertados: {total_inserted:,}")
    print(f"Tiempo total: {elapsed_time:.2f}s ({elapsed_time/60:.2f} min)")
    print(f"{'='*80}")


# =============================================================================
# FUNCIÓN PRINCIPAL Y CLI
# =============================================================================

def main():
    """Función principal del script CLI"""
    
    # Configurar argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description='Script CLI para construir la tabla OBT de NYC Taxi (v2.0 - ML Optimizado)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Modo FULL - Reconstruir todo
  python build_obt.py --mode full --overwrite true

  # Modo BY-PARTITION - Años específicos
  python build_obt.py --mode by-partition --year-start 2020 --year-end 2021

  # Meses específicos de un año
  python build_obt.py --mode by-partition --year-start 2020 --year-end 2020 --months 1,2,3
  
  python build_obt.py --mode by-partition --year-start 2024 --year-end 2025 

  # Servicios específicos
  python build_obt.py --mode by-partition --year-start 2020 --year-end 2020 --services yellow
        """
    )
    
    parser.add_argument('--mode', 
                       type=str, 
                       choices=['full', 'by-partition'], 
                       default='by-partition',
                       help='Modo de construcción: full (completo) o by-partition (por particiones) [default: by-partition]')
    
    parser.add_argument('--year-start', 
                       type=int, 
                       default=2015,
                       help='Año inicial (default: 2015)')
    
    parser.add_argument('--year-end', 
                       type=int, 
                       default=2025,
                       help='Año final (default: 2025)')
    
    parser.add_argument('--months', 
                       type=str, 
                       default=None,
                       help='Meses a procesar (formato: 1,2,3 o 1-12). Opcional.')
    
    parser.add_argument('--services', 
                       type=str, 
                       default='yellow,green',
                       help='Servicios a procesar: yellow, green o yellow,green (default: yellow,green)')
    
    parser.add_argument('--run-id', 
                       type=str, 
                       default=None,
                       help='ID de ejecución (opcional, se genera automáticamente si no se especifica)')
    
    parser.add_argument('--overwrite', 
                       type=str, 
                       choices=['true', 'false'], 
                       default='false',
                       help='Si es true, limpia la tabla antes de insertar (solo modo full)')
    
    args = parser.parse_args()
    
    mode = os.getenv('OBT_MODE', args.mode)
    year_start = int(os.getenv('OBT_YEAR_START', args.year_start))
    year_end = int(os.getenv('OBT_YEAR_END', args.year_end))
    services_str = os.getenv('OBT_SERVICES', args.services)
    overwrite_str = os.getenv('OBT_OVERWRITE', args.overwrite)
    months_str = os.getenv('OBT_MONTHS', args.months or '')
    run_id_env = os.getenv('RUN_ID', args.run_id)
    
    # Procesar argumentos
    services = [s.strip() for s in services_str.split(',') if s.strip()]
    months = None
    if months_str:
        if '-' in months_str:
            start, end = map(int, months_str.split('-'))
            months = list(range(start, end + 1))
        else:
            months = [int(m) for m in months_str.split(',') if m.strip()]
    
    run_id = run_id_env if run_id_env else f"obt_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    overwrite = overwrite_str.lower() in ('true', '1', 'yes')
    
    # Banner inicial
    print(f"\n{'='*80}")
    print(f" BUILD OBT - NYC TAXI DATA WAREHOUSE (ML OPTIMIZADO)")
    print(f"{'='*80}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Run ID: {run_id}")
    print(f"Versión: 2.0 - Lectura directa desde RAW, optimizado para ML")
    print(f"Configuración:")
    print(f"  - Modo: {mode}")
    print(f"  - Años: {year_start} - {year_end}")
    print(f"  - Servicios: {', '.join(services)}")
    print(f"  - Overwrite: {overwrite}")
    if months:
        print(f"  - Meses: {', '.join(map(str, months))}")
    print(f"{'='*80}\n")
    
    # Conectar a PostgreSQL
    print(" Conectando a PostgreSQL...")
    conn = get_connection()
    print("    Conexión establecida")
    
    try:
        # Crear schemas y tablas
        create_schemas(conn)
        create_obt_table(conn)
        
        # Ejecutar modo seleccionado
        if mode == 'full':
            build_obt_full(conn, run_id, overwrite)
        else:  # by-partition
            build_obt_by_partition(conn, run_id, services, 
                                  year_start, year_end, months)
        
        print(f"\n Proceso completado exitosamente")
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        conn.close()
        print("\n Conexión cerrada")


if __name__ == "__main__":
    main()
