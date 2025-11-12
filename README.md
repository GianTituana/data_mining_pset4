# Gian Tituaña, pset 4

## 📋 Descripción

Sistema completo de data warehouse para datos de NYC Taxi con:
- PostgreSQL para almacenamiento
- Jupyter + Spark para análisis
- OBT Builder para construcción de tablas analíticas
- ML pipeline para regresión de total_amount

## 🚀 Quick Start

### 1. Configuración Inicial

```bash
# Copiar archivo de variables de entorno
cp .env.example .env

# Editar .env con tus credenciales
# IMPORTANTE: Cambiar PG_PASSWORD por seguridad
nano .env
```

### 2. Levantar Servicios

```bash
# Iniciar todos los servicios
docker compose up -d

# Verificar que estén corriendo
docker compose ps
```

### 3. Ejecutar OBT Builder

```bash
# Opción 1: Construcción completa (2023-2025)
docker compose run --rm obt-builder python build_obt.py --mode full --overwrite true

# Opción 2: Por particiones (recomendado)
docker compose run --rm obt-builder python build_obt.py \
  --mode by-partition \
  --year-start 2023 \
  --year-end 2025 \
  --services yellow,green

# Opción 3: Meses específicos
docker compose run --rm obt-builder python build_obt.py \
  --mode by-partition \
  --year-start 2024 \
  --year-end 2024 \
  --months 1,2,3 \
  --services yellow
```

## 📦 Servicios Disponibles

### PostgreSQL (warehouse)
- **Puerto**: 5432
- **Usuario**: Definido en `.env` (`PG_USER`)
- **Password**: Definido en `.env` (`PG_PASSWORD`)
- **Database**: Definido en `.env` (`PG_DB`)

### PgAdmin (warehouseui)
- **URL**: http://localhost:8080
- **Email**: Definido en `.env` (`PGADMIN_DEFAULT_EMAIL`)
- **Password**: Definido en `.env` (`PGADMIN_DEFAULT_PASSWORD`)

### Jupyter + Spark (spark-notebook)
- **URL**: http://localhost:8888
- **Token**: Definido en `.env` (`JUPYTER_TOKEN`)
- **Spark UI**: http://localhost:4040 (cuando hay jobs corriendo)

### OBT Builder (obt-builder)
- **Ejecución manual**: `docker compose run obt-builder`
- **Logs**: Se guardan en `./logs/`

## 🔧 Variables de Entorno

### PostgreSQL
```env
PG_HOST=warehouse          # Nombre del servicio en Docker
PG_PORT=5432              # Puerto de PostgreSQL
PG_DB=postgres            # Nombre de la base de datos
PG_USER=postgres          # Usuario de PostgreSQL
PG_PASSWORD=tu_password   # ⚠️ CAMBIAR EN PRODUCCIÓN
PG_SCHEMA_RAW=raw         # Schema para datos raw
PG_SCHEMA_ANALYTICS=analytics  # Schema para OBT
```

### OBT Builder
```env
OBT_MODE=by-partition     # Modo: full o by-partition
OBT_YEAR_START=2023       # Año inicial
OBT_YEAR_END=2025         # Año final
OBT_MONTHS=               # Opcional: 1,2,3 o 1-12
OBT_OVERWRITE=false       # true para sobrescribir datos
SERVICES=yellow,green     # Servicios a procesar
```

## 📊 Estructura de Datos

### Schemas
- **raw**: Datos crudos de ingesta
  - `yellow_taxi_trip`
  - `green_taxi_trip`
  - `taxi_zone_lookup`

- **analytics**: Datos procesados
  - `obt_trips` (particionada por año/mes)

### Particiones OBT
```
analytics.obt_trips_2023_01
analytics.obt_trips_2023_02
...
analytics.obt_trips_2025_12
```

## 🛠️ Comandos Útiles

### Docker Compose

```bash
# Ver logs de un servicio
docker compose logs -f warehouse
docker compose logs -f spark-notebook

# Reiniciar un servicio
docker compose restart warehouse

# Parar todos los servicios
docker compose down

# Parar y eliminar volúmenes (⚠️ borra datos)
docker compose down -v

# Reconstruir imágenes
docker compose build obt-builder
```

### PostgreSQL

```bash
# Conectar a PostgreSQL desde terminal
docker compose exec warehouse psql -U postgres -d postgres

# Backup de la base de datos
docker compose exec warehouse pg_dump -U postgres postgres > backup.sql

# Restore de backup
cat backup.sql | docker compose exec -T warehouse psql -U postgres postgres
```

### Jupyter

```bash
# Ver token de Jupyter (si no se definió en .env)
docker compose logs spark-notebook | grep token
```

## 📝 Notebooks Disponibles

### 1. Ingesta de Datos (`01_ingesta_parquet_raw.ipynb`)
- Descarga y carga datos desde NYC TLC
- Almacena en tablas raw

### 2. ML Regression (`ml_total_amount_regression_new.ipynb`)
- Modelo de regresión para predecir total_amount
- Feature engineering
- Modelos: SGD, Ridge, Lasso, ElasticNet
- Evaluación y métricas

## 🔒 Seguridad

### En Desarrollo
```env
PG_PASSWORD=root
JUPYTER_TOKEN=
```

### En Producción
```env
PG_PASSWORD=contraseña_segura_aleatoria_123!
JUPYTER_TOKEN=token_aleatorio_456
PGADMIN_DEFAULT_PASSWORD=admin_password_789
```

## 📂 Estructura del Proyecto

```
.
├── docker-compose.yaml          # Orquestación de servicios
├── .env.example                 # Template de variables de entorno
├── .gitignore                   # Archivos ignorados por Git
├── build_obt.py                 # Script CLI para construir OBT
├── notebooks/
├── README.md
└── evidencias/             
```

## 🧪 Testing

### Verificar Conexión a PostgreSQL
```bash
docker compose exec warehouse psql -U postgres -d postgres -c "SELECT version();"
```

### Verificar Schemas
```bash
docker compose exec warehouse psql -U postgres -d postgres -c "\dn"
```

### Contar Registros en OBT
```bash
docker compose exec warehouse psql -U postgres -d postgres -c "SELECT COUNT(*) FROM analytics.obt_trips;"
```

### Ver Particiones
```bash
docker compose exec warehouse psql -U postgres -d postgres -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname='analytics' AND tablename LIKE 'obt_trips_%' ORDER BY tablename;"
```

## 🐛 Troubleshooting

### Error: "Connection refused" al conectar a PostgreSQL
```bash
# Verificar que el servicio esté corriendo
docker compose ps

# Ver logs de PostgreSQL
docker compose logs warehouse

# Reiniciar el servicio
docker compose restart warehouse
```

### Error: "Permission denied" en volúmenes
```bash
# Dar permisos a los directorios
chmod -R 777 warehouse warehouseui work logs
```

### OBT Builder no encuentra las tablas raw
```bash
# Verificar que existan las tablas
docker compose exec warehouse psql -U postgres -d postgres -c "\dt raw.*"

# Si no existen, ejecutar primero el notebook de ingesta
```

### Jupyter no carga
```bash
# Ver logs
docker compose logs spark-notebook

# Reiniciar
docker compose restart spark-notebook

# Obtener URL con token
docker compose logs spark-notebook | grep "http://127.0.0.1:8888"
```