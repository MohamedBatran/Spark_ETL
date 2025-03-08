# Top 1000 GitHub Repositories for Popular Topics

## Overview
This project implements an ETL (Extract, Transform, Load) data pipeline using Apache Spark and PostgreSQL. The pipeline processes JSON files containing data about the top-starred repositories on GitHub, extracts meaningful insights, and stores the transformed data in a relational database.

## Data
### Source
The dataset is available on Kaggle: [Top 1000 GitHub Repositories for Multiple Domains](https://www.kaggle.com/datasets/anshulmehtakaggl/top-1000-github-repositories-for-multiple-domains?select=Spark.json).

### Content
The dataset consists of multiple JSON files. Each file contains the top 1000 starred repositories for a specific search term (the filename represents the search term).

### Schema
Each JSON file contains objects with the following fields:
- `created`: Repository creation date
- `description`: Repository description
- `forks`: Number of forks
- `fullname`: Full repository name (e.g., `owner/repo_name`)
- `id`: Unique repository identifier
- `language`: Primary programming language used
- `open_issues`: Number of open issues
- `repo_name`: Repository name
- `stars`: Number of stars
- `subscribers`: Number of watchers (subscribers)
- `topics`: List of repository topics
- `type`: Type of repository owner (`User` or `Organization`)
- `username`: Repository owner's username

## Steps
### 1. Set Up Docker Containers
- Run a PostgreSQL database container:
  ```sh
  docker network create spark_postgres_net
  docker run --name postgres_db --network spark_postgres_net -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin123 -e POSTGRES_DB=github_repos -p 5432:5432 -d postgres
  ```
- Run a Jupyter Notebook container with PySpark:
  ```sh
  docker run --name pyspark_notebook --network spark_postgres_net -p 8888:8888 -v $(pwd):/home/jovyan/work -d jupyter/pyspark-notebook
  ```
- Run a pgAdmin container (optional, for GUI database management):
  ```sh
  docker run --name pgadmin -p 5050:80 -e PGADMIN_DEFAULT_EMAIL=admin@example.com -e PGADMIN_DEFAULT_PASSWORD=admin123 -d dpage/pgadmin4
  ```

### 2. Extract Data
- Load JSON files into Apache Spark DataFrames.

### 3. Transform Data
- Create three tables:
  - **`programming_lang`**: Stores the count of repositories per programming language.
  - **`organizations_stars`**: Stores the total stars accumulated by organization-type accounts.
  - **`search_terms_relevance`**: Stores the relevance score per search term using the formula:
    ```
    relevance_score = 1.5 * forks + 1.32 * subscribers + 1.04 * stars
    ```

### 4. Load Data
- Store transformed data into a PostgreSQL database using Spark's JDBC connector.

### 5. Run Queries & Perform EDA
- Use SQL queries to analyze stored data and gain insights.

## Stopping Containers
To stop the running containers without removing them:
```sh
docker stop postgres_db pyspark_notebook pgadmin
```
To start them again:
```sh
docker start postgres_db pyspark_notebook pgadmin
```

## Conclusion
This project demonstrates how to build a scalable ETL pipeline using Apache Spark and PostgreSQL, leveraging Docker containers for a reproducible environment.

