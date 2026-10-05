# bigdata-2

**LG14: investigación y despliegue de un repositorio Big Data con Docker**
Universidad Privada del Valle

**Integrantes:** Antonio Santiago Sanchez Angulo, Bonny Alberto Crespo Romero y Mateo Alejandro Valeriano Sanchez

Este repositorio documenta el despliegue y la prueba de **[panovvv/bigdata-docker-compose](https://github.com/panovvv/bigdata-docker-compose)**. Ese proyecto levanta, con un solo `docker compose up -d`, un clúster Big Data con **Hadoop (HDFS + YARN), Hive, Spark, Livy y Zeppelin**.

---

## 1. Repositorio seleccionado

| Campo | Dato |
|---|---|
| Nombre | bigdata-docker-compose |
| Autor / organización | Vadim Panov ([panovvv](https://github.com/panovvv)) |
| URL | https://github.com/panovvv/bigdata-docker-compose |
| Fecha de creación | 30 de agosto de 2019 |
| Última actualización | 4 de febrero de 2021 |
| Estrellas / forks | 168 / 78 |
| Licencia | MIT |
| Descripción | Entorno Docker Compose que despliega un clúster con Hadoop (HDFS/YARN), Hive, Spark, Zeppelin y Livy para procesar y consultar datos distribuidos. |

**Objetivo del proyecto:** ofrecer un "patio de juegos" de Big Data que se enciende y se apaga con un solo comando. Reproduce en una computadora personal las herramientas de Amazon EMR sin instalar cada una por separado.

## 2. Tecnologías

| Aspecto | Detalle |
|---|---|
| Tecnología principal | Apache Hadoop 3.2.0 (HDFS + YARN) |
| Sobre Hadoop | Apache Hive 3.1.2, Apache Spark 2.4.5 (Scala 2.11) |
| Frameworks adicionales | Apache Zeppelin 0.9.0 (notebooks), Apache Livy 0.7.0 (API REST para Spark), HiveServer2, Spark History Server |
| Base de datos | PostgreSQL 11.5 (Hive Metastore) |
| Imágenes Docker | `panovvv/hadoop-hive-spark:2.5.2`, `panovvv/livy:2.5.2`, `panovvv/zeppelin-bigdata:2.5.2`, `postgres:11.5` |
| Sistema operativo base | Alpine Linux 3.10 (PostgreSQL usa la imagen oficial, basada en Debian) |
| Lenguajes | Java 8, Scala 2.11, Python 3.7 (PySpark), R 3.6 (SparkR), HiveQL/SQL. En el repositorio: YAML, SQL y Python |

## 3. Arquitectura

### Diagrama

![Diagrama de arquitectura](docs/arquitectura.png)

Fuente editable: [`docs/arquitectura.svg`](docs/arquitectura.svg)

El clúster sigue el modelo **maestro / trabajadores**:
- `master` concentra los servicios que coordinan: NameNode, ResourceManager, Spark Master y HiveServer2.
- `worker1` y `worker2` guardan los datos (DataNode) y ejecutan las tareas (NodeManager y Spark Worker).
- Alrededor hay tres servicios de apoyo: PostgreSQL para el catálogo de Hive, Livy (API REST) y Zeppelin (notebooks).

### Contenedores

Docker Compose crea **6 contenedores**, llamados `<carpeta>-<servicio>-1` (por ejemplo `bigdata-2-master-1`).

| Servicio | Imagen | IP (spark_net) | Función | Puertos publicados en el host |
|---|---|---|---|---|
| `hivemetastore` | postgres:11.5 | 172.28.1.4 | Base de datos del Hive Metastore (catálogo de bases de datos, tablas y esquemas de Hive) | ninguno (5432 solo dentro de la red) |
| `master` | panovvv/hadoop-hive-spark:2.5.2 | 172.28.1.1 | NameNode y Secondary NameNode (HDFS), ResourceManager (YARN), Spark Master, Spark History Server y HiveServer2 | 9870, 9868, 8088, 8180→8080, 4040-4043, 18080, 10000 |
| `worker1` | panovvv/hadoop-hive-spark:2.5.2 | 172.28.1.2 | DataNode (guarda los bloques), NodeManager y Spark Worker | 9864, 8081 |
| `worker2` | panovvv/hadoop-hive-spark:2.5.2 | 172.28.1.3 | Igual que worker1. Permite replicar los bloques con factor 2 | 9865, 8082 |
| `livy` | panovvv/livy:2.5.2 | 172.28.1.6 | API REST para enviar sesiones y trabajos Spark a YARN | 8998 |
| `zeppelin` | panovvv/zeppelin-bigdata:2.5.2 | 172.28.1.5 | Notebooks web para Spark (Scala, PySpark, SQL) y Hive | 8890 |

`master`, `worker1`, `worker2` y `livy` declaran `expose: 1-65535`: todos sus puertos son accesibles **dentro** de la red. Por ejemplo, el puerto 7077 del Spark Master, al que se conectan los workers, no se publica al host.

### Red

- Una sola red personalizada, `spark_net` (driver bridge, IPAM con subred `172.28.0.0/16`). Docker la nombra `<carpeta>_spark_net`.
- Cada contenedor tiene una **IP fija** (`ipv4_address`), de 172.28.1.1 a 172.28.1.6.
- Cada servicio declara `extra_hosts`, que agrega al `/etc/hosts` los nombres `master`, `worker1`, `worker2`, `hivemetastore`, `livy` y `zeppelin`. Es necesario porque Hadoop y Spark identifican a los nodos por nombre de host.

### Volúmenes

No hay volúmenes con nombre; solo **bind mounts** (carpetas del repositorio montadas en los contenedores):

| Origen (host) | Destino (contenedor) | Contenedores | Uso |
|---|---|---|---|
| `./data` | `/data` | master, worker1, worker2, livy, zeppelin | Datos de ejemplo (`grades.csv`, `ssn-address.tsv`) y `batches/sample_batch.py` |
| `./init.sql` | `/docker-entrypoint-initdb.d/init.sql` | hivemetastore | PostgreSQL lo ejecuta en el primer arranque y crea la BD `hivemetastoredb` |
| `./livy_batches` | `/livy_batches` | livy | Trabajos batch para Livy |
| `./zeppelin_notebooks` | `/zeppelin_notebooks` | zeppelin | Notebooks de Zeppelin (persisten en el host) |

Consecuencia: los bloques y metadatos de HDFS y la base del metastore viven **dentro** de los contenedores. Se conservan con `docker compose stop`, pero **se pierden con `docker compose down`**.

### Dependencias entre servicios (`depends_on`)

```
hivemetastore ──► master, worker1, worker2 ──► livy ──► zeppelin
```

| Servicio | Arranca después de |
|---|---|
| `master`, `worker1`, `worker2` | `hivemetastore` |
| `livy` | `master`, `worker1`, `worker2` |
| `zeppelin` | `master`, `worker1`, `worker2`, `livy` |

`hivemetastore` además tiene un **healthcheck** (`pg_isready -U postgres` cada 10 s, 5 reintentos). `depends_on` solo controla el orden de arranque; no espera a que cada servicio esté listo.

### Variables de entorno

| Servicio | Variable | Valor | Para qué sirve |
|---|---|---|---|
| hivemetastore | `POSTGRES_PASSWORD` | `new_password` | Clave del usuario `postgres` |
| master | `HADOOP_NODE` | `namenode` | Hace que la imagen arranque como nodo maestro (NameNode, ResourceManager…) |
| master | `HIVE_CONFIGURE` | `yes, please` | Configura e inicia Hive (HiveServer2 + metastore en PostgreSQL) |
| master | `SPARK_MASTER_HOST`, `SPARK_LOCAL_IP` | `172.28.1.1` | IP en la que escucha el Spark Master |
| master / workers | `SPARK_LOCAL_HOSTNAME` | `master` / `worker1` / `worker2` | Nombre con el que Spark se anuncia |
| master / workers | `SPARK_PUBLIC_DNS` | `localhost` | Para que los enlaces de las UIs de Spark apunten a localhost |
| workers | `HADOOP_NODE` | `datanode` | Arranca DataNode y NodeManager |
| workers | `SPARK_MASTER_ADDRESS` | `spark://master:7077` | Master al que se registra el Spark Worker |
| workers | `SPARK_WORKER_PORT` / `SPARK_WORKER_WEBUI_PORT` | 8881/8081 y 8882/8082 | Puertos distintos para cada worker |
| worker2 | `HADOOP_DATANODE_UI_PORT` | `9865` | Evita el choque con el 9864 de worker1 en el host |
| livy | `SPARK_MASTER` | `yarn` | Livy envía los trabajos Spark a YARN |
| livy | `LOCAL_DIR_WHITELIST` | `/data/batches/` | Carpeta desde la que Livy puede ejecutar scripts |
| livy | `ENABLE_HIVE_CONTEXT` | `false` | Sesiones Spark sin soporte de Hive |
| zeppelin | `ZEPPELIN_PORT` | `8890` | Puerto de la interfaz web |
| zeppelin | `ZEPPELIN_NOTEBOOK_DIR` | `/zeppelin_notebooks` | Dónde guarda los notebooks (bind mount) |

### Archivos de configuración

| Archivo | Descripción |
|---|---|
| [`docker-compose.yml`](docker-compose.yml) | Define los 6 servicios, sus imágenes, puertos, variables de entorno, volúmenes, dependencias y la red `spark_net`. Usa `version: "3.7"`; Compose v2 avisa que ese atributo es obsoleto y lo ignora |
| [`init.sql`](init.sql) | `CREATE DATABASE "hivemetastoredb";`: crea la base que usa el Hive Metastore |
| [`data/batches/sample_batch.py`](data/batches/sample_batch.py) | Trabajo PySpark de ejemplo para enviar con Livy |
| `*-site.xml` (dentro de la imagen) | `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `hive-site.xml` y `spark-defaults.conf` no están en este repositorio: vienen dentro de la imagen `panovvv/hadoop-hive-spark`, y el script de arranque (`/entrypoint.sh`) aplica las variables de entorno de cada servicio |

## 4. Estructura del repositorio

```
bigdata-2/
├── docker-compose.yml     # Definición del clúster (6 servicios, red spark_net)
├── init.sql               # Crea la BD hivemetastoredb en PostgreSQL
├── data/                  # Montado en /data
│   ├── grades.csv
│   ├── ssn-address.tsv
│   └── batches/sample_batch.py
└── docs/
    ├── arquitectura.png   # Diagrama de arquitectura
    └── arquitectura.svg   # Diagrama (fuente editable)
```

## 5. Instalación y ejecución

**Requisitos:** Docker Desktop con Docker Compose v2, unos 10 GB libres para las imágenes y **al menos 8 GB de RAM asignados a Docker**, como recomienda el autor del proyecto original.

```bash
# 1. Clonar e ingresar al repositorio
git clone https://github.com/MateoVal007/bigdata-2.git
cd bigdata-2

# 2. Identificar los archivos principales y revisar la configuración
ls -la
cat docker-compose.yml

# 3. Construir y ejecutar (descarga las imágenes la primera vez)
docker compose up -d

# 4. Verificar los contenedores
docker ps

# 5. Entrar al nodo maestro para usar HDFS
docker compose exec master bash

# 6. Apagar el clúster (borra los datos de HDFS)
docker compose down
```

Interfaces web:

| Servicio | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Spark Master | http://localhost:8180 |
| Spark History Server | http://localhost:18080 |
| DataNodes | http://localhost:9864 · http://localhost:9865 |
| Spark Workers | http://localhost:8081 · http://localhost:8082 |
| Zeppelin | http://localhost:8890 |
| Livy | http://localhost:8998 |

## 6. Prueba funcional (HDFS)

Comandos ejecutados dentro del contenedor `master`:

```bash
hdfs dfsadmin -report                              # estado del clúster
hdfs dfsadmin -safemode get
hdfs dfs -mkdir -p /user/grupo/prueba              # 1. crear un directorio
hdfs dfs -ls /user/grupo
hdfs dfs -put /data/grades.csv /user/grupo/prueba/ # 2. cargar un archivo
hdfs dfs -ls /user/grupo/prueba                    # 3. consultar el archivo
hdfs dfs -cat /user/grupo/prueba/grades.csv | head
hdfs fsck /user/grupo/prueba -files -blocks -locations
```

| # | Objetivo | Resultado obtenido |
|---|---|---|
| 1 | Estado del clúster | 2 DataNodes vivos: worker1 (172.28.1.2) y worker2 (172.28.1.3). `Safe mode is OFF` |
| 2 | **Crear un directorio** | `drwxr-xr-x root supergroup 0 2026-10-04 21:09 /user/grupo/prueba` |
| 3 | **Cargar un archivo** | `grades.csv` copiado desde `/data` (bind mount de `./data`) a HDFS sin errores |
| 4 | **Consultar el archivo** | `-rw-r--r-- 2 root supergroup 764 /user/grupo/prueba/grades.csv`. `-cat` muestra la cabecera `Last name,First name,SSN,Test1,…,Grade` y los registros |
| 5 | Bloques y réplicas | 1 bloque, `replication=2`, réplicas en `172.28.1.2:9866` y `172.28.1.3:9866`. **Status: HEALTHY** |
| 6 | Interfaz web (http://localhost:9870) | worker1 y worker2 *In service*. `/user/grupo/prueba/grades.csv`: 764 B, Replication 2, Block Size 128 MB |

**Análisis**
- El NameNode solo guarda metadatos. El contenido está en los DataNodes, y `fsck` muestra el bloque copiado en **los dos workers**: el almacenamiento está distribuido entre contenedores distintos.
- El factor de replicación es 2, igual al número de DataNodes. Si un worker falla, el archivo sigue disponible.
- El archivo ocupa un solo bloque de 764 bytes. HDFS no reserva los 128 MB del tamaño de bloque.
- Los avisos `SLF4J: multiple bindings` y `Unable to load native-hadoop library` son informativos. El estado `unhealthy` que muestra `docker ps` en algunos contenedores viene del healthcheck de la imagen y no afectó a HDFS.

## 7. Comparación con big-data-europe/docker-hadoop

| Característica | [docker-hadoop](https://github.com/big-data-europe/docker-hadoop) | bigdata-docker-compose (seleccionado) |
|---|---|---|
| Tecnología principal | Hadoop 3.2.1: HDFS, YARN, MapReduce | Hadoop 3.2.0 (HDFS, YARN) + Hive 3.1.2 + Spark 2.4.5 |
| Docker | Una imagen por rol (`bde2020/hadoop-*`), Debian 9 | Una imagen para todos los roles + Livy + Zeppelin + Postgres, Alpine 3.10 |
| Docker Compose | Sí: 5 servicios, red por defecto, config. en `hadoop.env` | Sí: 6 servicios, red `spark_net` con IP fijas, config. en `environment` |
| Número de contenedores | 5 | 6 |
| Almacenamiento distribuido | HDFS: 1 NameNode + 1 DataNode | HDFS: 1 NameNode + 2 DataNodes, replicación 2 |
| Procesamiento distribuido | YARN + MapReduce | YARN + Spark (2 workers), Hive SQL, Livy |
| Interfaces web | Solo 9870 publicada | Todas publicadas en localhost |
| Persistencia | Volúmenes con nombre para HDFS | Solo bind mounts; HDFS se pierde con `down` |
| Complejidad de instalación | Baja | Media (imágenes pesadas, unos 8 GB de RAM) |
| Documentación | README breve | README extenso con pruebas por componente |
| Caso de uso | Aprender el núcleo de Hadoop | Laboratorio completo tipo Amazon EMR |

## 8. Créditos

- Proyecto original: [panovvv/bigdata-docker-compose](https://github.com/panovvv/bigdata-docker-compose), de Vadim Panov (licencia MIT). `docker-compose.yml`, `init.sql` y `data/` provienen de ese repositorio.
- Referencia de comparación: [big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop).
