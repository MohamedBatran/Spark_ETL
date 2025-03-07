# GitHub Repositories ETL Pipeline

## Overview
This project builds an **ETL (Extract, Transform, Load) data pipeline** using **Apache Spark** and **PostgreSQL**. The pipeline processes JSON files containing data on GitHub repositories, extracts relevant information, transforms the data, and loads it into a structured database.

## Steps
### 1. **Setup Environment**
- Use `jupyter/pyspark-notebook` Docker container to run PySpark.
- Use `PostgreSQL` as the database.
- Use `pgAdmin` as the database GUI.

### 2. **Docker Setup**
#### Create a network for the containers:
```sh
docker network create spark_postgres_net
```
#### Run a PostgreSQL container:
```sh
docker run --name postgres_db \
    --network spark_postgres_net \
    -e POSTGRES_USER=admin \
    -e POSTGRES_PASSWORD=admin123 \
    -e POSTGRES_DB=github_data \
    -p 5432:5432 \
    -d postgres
```
#### Run a Jupyter PySpark container:
```sh
docker run --name pyspark_notebook \
    --network spark_postgres_net \
    -p 8888:8888 \
    -v $(pwd):/home/jovyan/work \
    -d jupyter/pyspark-notebook
```
#### Run a pgAdmin container:
```sh
docker run --name pgadmin \
    --network spark_postgres_net \
    -p 5050:80 \
    -e PGADMIN_DEFAULT_EMAIL=admin@example.com \
    -e PGADMIN_DEFAULT_PASSWORD=admin123 \
    -d dpage/pgadmin4
```
#### Accessing the services:
- **Jupyter Notebook**: Open `http://localhost:8888`
- **pgAdmin**: Open `http://localhost:5050`

### 3. **Extract Data**
- Load JSON files containing data about GitHub repositories.
- Extract relevant fields such as `stars`, `forks`, `subscribers`, `language`, `organization`, and `search term`.

### 4. **Transform Data**
- Compute aggregated metrics:
  - Total repositories per programming language.
  - Total stars per organization.
  - Relevance score for each search term (`1.5 * forks + 1.32 * subscribers + 1.04 * stars`).

### 5. **Load Data**
- Store the processed data into a **PostgreSQL database**.
- Tables created:
  1. **programming_lang**: Stores programming languages and their repository count.
  2. **organizations_stars**: Stores organizations and their total stars.
  3. **search_terms_relevance**: Stores search terms and their computed relevance score.

### 6. **Verify Data in PostgreSQL**
- Use `pgAdmin` or `psql` to check the tables and verify the inserted data.
- Run SQL queries to validate the transformations.

### 7. **Shutdown and Cleanup**
To stop and remove containers:
```sh
docker stop postgres_db pyspark_notebook pgadmin

docker rm postgres_db pyspark_notebook pgadmin
```
To remove the network:
```sh
docker network rm spark_postgres_net
```

