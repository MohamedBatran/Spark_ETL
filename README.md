# GitHub Repository Analysis with PySpark & PostgreSQL

## 📌 Project Overview
This project analyzes GitHub repository data using **PySpark** and stores the results in **PostgreSQL**. It utilizes **Docker** to run PostgreSQL and Jupyter/PySpark in separate containers, allowing easy deployment and scalability.

## 🛠️ Setup Instructions

### 1️⃣ Install Docker (if not installed)
Download and install Docker from [here](https://www.docker.com/get-started).

### 2️⃣ Run PostgreSQL Container
```sh
docker network create spark_postgres_net  # Create a network

docker run --name postgres_db --network spark_postgres_net \
    -e POSTGRES_USER=admin \
    -e POSTGRES_PASSWORD=admin123 \
    -e POSTGRES_DB=github_analysis \
    -p 5432:5432 -d postgres
```

### 3️⃣ Run pgAdmin Container (Optional, for GUI)
```sh
docker run --name pgadmin --network spark_postgres_net -p 5050:80 \
    -e PGADMIN_DEFAULT_EMAIL=admin@example.com \
    -e PGADMIN_DEFAULT_PASSWORD=admin123 \
    -d dpage/pgadmin4
```
- Access **pgAdmin** at `http://localhost:5050`
- Add **PostgreSQL Server** using:
  - Hostname: `postgres_db`
  - Port: `5432`
  - Username: `admin`
  - Password: `admin123`

### 4️⃣ Run Jupyter Notebook with PySpark
```sh
docker run --name pyspark_notebook --network spark_postgres_net -p 8888:8888 -v $(pwd):/home/jovyan/work \
    jupyter/pyspark-notebook
```
- Access **Jupyter Notebook** at `http://localhost:8888`
- Open the project notebook from the `work` directory

## 🔄 Data Processing Pipeline
### 1️⃣ Load GitHub Repository Data
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GitHubRepoAnalysis") \
    .config("spark.jars", "/postgresql-42.7.5.jar") \
    .getOrCreate()

df = spark.read.json("github_repos.json")
df.printSchema()
```

### 2️⃣ Transform Data (Example: Sum Stars per Organization)
```python
from pyspark.sql.functions import sum

org_stars_df = df.filter(df["type"] == "Organization") \
    .groupBy("username") \
    .agg(sum("stars").alias("total_stars"))
```

### 3️⃣ Save Data to PostgreSQL
```python
jdbc_url = "jdbc:postgresql://postgres_db:5432/github_analysis"
org_stars_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "organizations_stars") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
```

## 📊 Exploratory Data Analysis (EDA)
- Check data summary:
```python
df.describe().show()
```
- Compute a relevance score:
```python
from pyspark.sql.functions import col

df = df.withColumn("relevance_score", 1.5 * col("forks") + 1.32 * col("subscribers") + 1.04 * col("stars"))
df.select("repo_name", "relevance_score").show()
```

## ✅ Next Steps
- **Enhance data cleaning** (handle missing values, remove duplicates)
- **Add visualization** (use Power BI, Matplotlib, or Seaborn for insights)
- **Deploy as an ETL pipeline** using Apache Airflow

---

### 📝 Author: Mohamed Batran

