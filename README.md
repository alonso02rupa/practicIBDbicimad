
# 🏙️ Proyecto de Infraestructura de Datos para Madrid

Este repositorio contiene la implementación de una infraestructura de datos integrada para la ciudad de Madrid, diseñada para tres tipos de usuario:  
👨‍🔬 Científicos de datos  
🏛️ Gestores municipales  
👥 Ciudadanos y asociaciones vecinales  

La solución combina un **Data Lake basado en MinIO** y un **Data Warehouse en PostgreSQL**, habilitando análisis avanzados, consultas SQL y visualizaciones intuitivas sobre datos de movilidad, participación ciudadana e infraestructura urbana.

---

## 📑 Tabla de Contenidos

1. [📊 Diagrama de la Infraestructura](#diagrama-de-la-infraestructura)
2. [🧩 Modelo de Datos Diseñado](#modelo-de-datos-diseñado)
3. [⚙️ Procesos de Transformación Implementados](#procesos-de-transformación-implementados)
4. [🚀 Guía de Puesta en Marcha](#guía-de-puesta-en-marcha)
5. [🧪 Ejemplos de Uso y Soporte a las Consultas](#ejemplos-de-uso-y-soporte-a-las-consultas)
6. [📚 Detalle de las Fuentes Adicionales Incorporadas y su Valor Añadido](#detalle-de-las-fuentes-adicionales-incorporadas-y-su-valor-añadido)

---

## 📊 Diagrama de la Infraestructura

La infraestructura se compone de un Data Lake con MinIO dividido en tres zonas principales y un Data Warehouse en PostgreSQL conectado a Apache Superset:

- **Raw Ingestion Zone**: Almacena datos en su formato original.
- **Process Zone**: Contiene datos limpios en formato Parquet.
- **Access Zone**: Datos enriquecidos para consultas, incluyendo el Data Warehouse.

### Diagrama Conceptual

```mermaid
graph TD
    A[Datos Fuentes] -->|Ingesta| B[Raw Ingestion Zone - MinIO]
    B -->|Procesamiento| C[Process Zone - MinIO]
    C -->|Enriquecimiento| D[Access Zone - MinIO]
    D -->|Carga| E[Data Warehouse - PostgreSQL]
    E -->|Consultas SQL y Visualizaciones| F[Apache Superset]
    C -->|Análisis Python| G[Notebooks]
```

---

## 🧩 Modelo de Datos Diseñado

El modelo en PostgreSQL sigue un **esquema de estrella** con tablas de hechos y dimensiones.

### 🧱 Tablas de Dimensiones

#### `dim_distritos`
- `id` (PK, INT)
- `nombre` (VARCHAR)
- `densidad_poblacion` (FLOAT)

#### `dim_tipos_usuario`
- `id` (PK, SERIAL)
- `tipo_usuario` (VARCHAR)

#### `dim_tipos_estacion`
- `id` (PK, SERIAL)
- `tipo_estacion` (VARCHAR)

#### `dim_aparcamientos`
- `id` (PK, INT)
- `nombre` (VARCHAR)
- `capacidad_total` (INT)
- `distrito_id` (FK a `dim_distritos`)

#### `dim_date_time`
- `id` (PK, SERIAL)
- `fecha_hora` (TIMESTAMP)
- `fecha` (DATE)
- `hora` (INT)
- `dia_semana` (VARCHAR)
- `numero_dia_semana` (INT)
- `es_festivo` (BOOLEAN)
- `mes` (INT)
- `trimestre` (INT)
- `año` (INT)

### 📊 Tablas de Hechos

#### `fact_usos_bicimad`
- `id_uso` (PK, SERIAL)
- `estacion_origen_id` (INT)
- `estacion_destino_id` (INT)
- `tipo_usuario_id` (FK)
- `duracion_segundos`, `distancia_km`, `calorias_estimadas`, `co2_evitado_gramos`

#### `fact_infraestructura`
- `distrito_id`, `tipo_estacion_id` (PK compuesta)
- `cantidad` (INT)

#### `fact_ocupacion_parkings`
- `aparcamiento_id`, `date_time_id` (PK compuesta)
- `plazas_ocupadas`, `porcentaje_ocupacion` (INT/FLOAT)

---

## ⚙️ Procesos de Transformación Implementados

### `01_ingest_data.py` – Ingesta
- Carga de archivos `.csv` y `.sql` en la zona Raw en MinIO.

### `02_process_data.py` – Procesamiento
- Limpieza y estandarización
- Conversión a formato Parquet
- Extracción desde SQL dump con SQLite

### `03_access_zone.py` – Enriquecimiento y Carga
- Agregado de columnas clave (e.g. `distrito_id`)
- Cálculo de métricas como `porcentaje_ocupacion`
- Carga en PostgreSQL

### `04_govern_zone.py` – Gobernanza
- Gestión de metadatos, linaje, seguridad y calidad

---

## 🚀 Guía de Puesta en Marcha

### 1️⃣ Clonar el repositorio
```bash
git clone https://github.com/tu_usuario/tu_repositorio.git
cd tu_repositorio
```

### 2️⃣ Requisitos
- Docker + Docker Compose

### 3️⃣ Levantar los servicios
```bash
docker-compose up -d
```

Esto iniciará MinIO, PostgreSQL, Superset y ejecutará los scripts automáticamente.

### 4️⃣ Acceder a Superset
- URL: http://localhost:8088  
- Usuario: `admin`  
- Contraseña: `admin`

### 5️⃣ Configurar PostgreSQL en Superset
- URI: `postgresql+psycopg2://postgres:postgres@localhost:5432/postgres`

### 6️⃣ Verificar los datos
- MinIO: http://localhost:9000 (usuario/contraseña: `minioadmin`)
- PostgreSQL: `localhost:5432`

---

## 🧪 Ejemplos de Uso y Soporte a las Consultas

### 🔬 Objetivo 1: Científicos de Datos

**Pregunta**: ¿Cuáles son los horarios de mayor congestión de tráfico?

**Método**: Análisis con Python + Pandas desde Process Zone

```python
traffic_data = download_dataframe_from_minio('access-zone', 'trafico/cleaned_traffic.parquet', format='parquet')
traffic_data.groupby('hora').agg({
    'coches': 'sum', 'motos': 'sum', 'camiones': 'sum', 'buses': 'sum', 'total_vehiculos': 'sum'
}).sort_values(by='total_vehiculos', ascending=False).head(10)
```

---

### 🏛️ Objetivo 2: Gestores Municipales

**1. Rutas de BiciMAD más populares**

```sql
SELECT estacion_origen_id, estacion_destino_id, COUNT(*) AS total_viajes
FROM fact_usos_bicimad
GROUP BY estacion_origen_id, estacion_destino_id
ORDER BY total_viajes DESC
LIMIT 10;
```

**2. Uso por tipo de usuario**

```sql
SELECT tipo_usuario, COUNT(*) AS total_viajes,
       ROUND(AVG(duracion_segundos) / 60, 1) AS duracion_media_min,
       ROUND(AVG(distancia_km), 2) AS distancia_media_km,
       ROUND(AVG(calorias_estimadas), 1) AS calorias_medias,
       ROUND(AVG(co2_evitado_gramos), 1) AS co2_medio
FROM fact_usos_bicimad
JOIN dim_tipos_usuario ON dim_tipos_usuario.id = fact_usos_bicimad.tipo_usuario_id
GROUP BY tipo_usuario;
```

**3. Relación entre densidad poblacional e infraestructura**

```sql
SELECT d.nombre AS distrito, d.densidad_poblacion,
       SUM(f.cantidad) AS numero_paradas
FROM dim_distritos d
LEFT JOIN fact_infraestructura f ON d.id = f.distrito_id
GROUP BY d.nombre, d.densidad_poblacion
ORDER BY d.densidad_poblacion DESC;
```

---

### 👥 Objetivo 3: Ciudadanos y Asociaciones Vecinales

**Pregunta**: ¿Qué parkings tienen mayor variación en su ocupación?

**Método**: Visualización en Superset

```sql
SELECT a.id AS aparcamiento_id, a.nombre AS nombre_aparcamiento,
       d.nombre AS distrito, d.densidad_poblacion,
       STDDEV(f.porcentaje_ocupacion) AS desviacion_ocupacion,
       AVG(f.porcentaje_ocupacion) AS ocupacion_media,
       COUNT(f.porcentaje_ocupacion) AS registros_totales
FROM fact_ocupacion_parkings f
JOIN dim_aparcamientos a ON f.aparcamiento_id = a.id
JOIN dim_distritos d ON a.distrito_id = d.id
JOIN dim_date_time dt ON f.date_time_id = dt.id
GROUP BY a.id, a.nombre, d.nombre, d.densidad_poblacion
ORDER BY desviacion_ocupacion DESC;
```

---

## 📚 Detalle de las Fuentes Adicionales Incorporadas y su Valor Añadido

### 📄 `ext_aparcamientos_info.csv`
- **Contenido**: Información de parkings (nombre, capacidad, ubicación)
- **Valor**: Enriquecimiento de datos de ocupación y contexto urbano

### 🗃️ `dump-bbdd-municipal.sql`
- **Contenido**: Datos municipales (distritos, infraestructuras, consumo)
- **Valor**: Contexto geográfico y estructural clave para análisis urbanos

---

¡Con esta infraestructura, Madrid da un paso hacia una gestión de datos urbana más transparente, eficiente y participativa! 🌐🚴‍♀️🚦
