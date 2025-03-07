# GitHub Repo ETL Pipeline

## Overview
This project builds an **ETL (Extract, Transform, Load) data pipeline** using **Apache Spark** and **PostgreSQL**. The pipeline processes JSON files containing data about the most-starred GitHub repositories, extracts key information, transforms it into structured formats, and loads it into a relational database for further analysis.

## Project Steps

### 1. **Set Up Environment**
- Use Docker to run **Jupyter/PySpark Notebook** and **PostgreSQL** in separate containers.
- Ensure both containers are in the same Docker network for seamless communication.

### 2. **Extract Data**
- Read multiple JSON files containing GitHub repository data using Apache Spark.
- Load the dataset into a Spark DataFrame.

### 3. **Transform Data**
- Extract relevant fields such as programming language, organization, and search terms.
- Calculate metrics like total stars per organization and a custom relevance score.
- Handle missing or corrupted records.

### 4. **Load Data into PostgreSQL**
- Create three structured tables:
  - `programming_lang`: Stores programming languages and the number of repositories using them.
  - `organizations_stars`: Stores total stars for repositories grouped by organization.
  - `search_terms_relevance`: Stores the relevance score of repositories based on forks, subscribers, and stars.
- Use **JDBC connection** to write transformed data into PostgreSQL.

### 5. **Perform Exploratory Data Analysis (EDA)**
- Inspect the data using Spark functions.
- Identify trends in programming languages, most-starred organizations, and repository relevance.

### 6. **Save and Document the Project**
- Push the project to **GitHub**, including all scripts and setup instructions.
- Document the process in this **README** file.

## Prerequisites
- **Docker** installed and configured.
- Basic knowledge of **Apache Spark**, **PostgreSQL**, and **Jupyter Notebooks**.

This pipeline automates the data ingestion and structuring process, making it easier to analyze trends in GitHub repository data. 🚀

