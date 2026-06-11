# MovieLens Big Data Analytics using Apache Spark, Cassandra, HBase and MongoDB

## Project Overview

This project explores the use of Apache Spark for large-scale data analytics on the MovieLens 100K dataset and demonstrates how analytical results can be stored and managed using different NoSQL databases, namely Apache Cassandra, Apache HBase and MongoDB.

The project focuses on distributed data processing, data storage and visualization to gain insights into movie ratings, user preferences and demographic characteristics. The implementation was conducted on Hortonworks Data Platform (HDP) 2.6.5 using Apache Zeppelin as the development environment.

---

## Objectives

The objectives of this project are:

- To process and analyze the MovieLens dataset using Apache Spark.
- To perform data cleansing and transformation using Spark SQL and DataFrames.
- To store analytical results in Cassandra, HBase and MongoDB.
- To compare the implementation of different NoSQL databases.
- To generate meaningful visualizations from the analytical results.
- To demonstrate the integration of Big Data technologies within a unified environment.

---

## Dataset Description

The MovieLens 100K dataset contains movie ratings collected from users and includes information about movies, users and ratings.

Dataset Source:

https://grouplens.org/datasets/movielens/

Files Used:

| File | Description |
|--------|--------|
| u.data | User ratings data |
| u.item | Movie information |
| u.user | User demographic information |

Dataset Characteristics:

- 100,000 ratings
- 943 users
- 1,682 movies
- Ratings range from 1 to 5

---

# Software Environment

| Component | Version |
|------------|------------|
| Operating System | HDP Sandbox 2.6.5 |
| Python | 2.7.5 |
| Apache Spark | 2.3.2 |
| Apache Zeppelin | 0.7.3 |
| Hadoop | 2.7.x |
| Apache Cassandra | 3.11.13 |
| Apache HBase | 1.1.2 |
| MongoDB | 3.2.22 |
| Java | JDK 1.8 |

---

# Libraries and Packages

The following libraries were used throughout the project:

```python
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import *
```

Additional Packages:

- Pandas 0.24.2
- Matplotlib 2.2.5
- PySpark
- Spark SQL
- DataStax Spark Cassandra Connector

---

# Project Architecture

```text
MovieLens Dataset
        │
        ▼
      HDFS
        │
        ▼
 Apache Spark
(Data Processing)
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Cassandra HBase MongoDB
        │
        ▼
 Data Visualization
```

---

# Reproducibility Instructions

## Step 1: Start Hadoop Services

Verify Hadoop services:

```bash
jps
```

Expected services:

```text
NameNode
DataNode
ResourceManager
NodeManager
```

---

## Step 2: Upload Dataset to HDFS

Create project directory:

```bash
hdfs dfs -mkdir -p /user/maria_dev/movielens
```

Upload dataset files:

```bash
hdfs dfs -put u.data /user/maria_dev/movielens/
hdfs dfs -put u.item /user/maria_dev/movielens/
hdfs dfs -put u.user /user/maria_dev/movielens/
```

Verify upload:

```bash
hdfs dfs -ls /user/maria_dev/movielens
```

---

## Step 3: Launch Apache Zeppelin

Open Apache Zeppelin:

```text
http://127.0.0.1:9995
```

Create a new notebook and select the Spark interpreter.

---

# Data Processing Workflow

The following workflow was implemented:

1. Load MovieLens files from HDFS.
2. Create Spark DataFrames.
3. Perform data cleansing and preprocessing.
4. Register temporary views.
5. Execute Spark SQL queries.
6. Generate analytical results.
7. Store results in Cassandra.
8. Store results in HBase.
9. Store results in MongoDB.
10. Generate visualizations.

---

# Analytical Tasks

## Task 1: Average Movie Ratings

Objective:

Calculate the average rating received by each movie.

Output:

| movie_id | average_rating |
|-----------|-----------|
| 829 | 2.65 |
| 1436 | 2.50 |
| 467 | 3.79 |

---

## Task 2: Top Rated Movies

Objective:

Identify movies with the highest average ratings and significant rating counts.

Output Fields:

- Movie Title
- Total Ratings
- Average Rating

---

## Task 3: Most Active Users and Favourite Genres

Objective:

Determine the most active users based on the number of ratings submitted and identify their favourite genres.

Output Fields:

- User ID
- Total Movies Rated
- Favourite Genre

---

## Task 4: Users Below 20 Years Old

Objective:

Analyze demographic characteristics of users below 20 years old.

Output Fields:

- User ID
- Age
- Gender
- Occupation

---

## Task 5: Scientists Aged 30–40

Objective:

Analyze scientists aged between 30 and 40 years old and examine their favourite movie genres.

Output Fields:

- User ID
- Age
- Gender
- Occupation

---

# Cassandra Implementation

## Keyspace Creation

```sql
CREATE KEYSPACE movielen
WITH replication = {
    'class':'SimpleStrategy',
    'replication_factor':1
};
```

## Tables

```text
average_rating
top10_movies
active_favourite_genre
young_users
scientist_users
```

## Verification

```sql
DESCRIBE TABLES;
```

---

# HBase Implementation

## Tables

```bash
create 'average_rating','cf'
create 'top10_movies','cf'
create 'active_favourite_genre','cf'
create 'young_users','cf'
create 'scientist_users','cf'
```

## Verification

```bash
list
```

## Query Example

```bash
scan 'average_rating'
```

---

# MongoDB Implementation

## Database

```javascript
use movielen
```

## Collections

```text
average_rating
top10_movies
active_favourite_genre
young_users
scientist_users
```

## Verification

```javascript
show collections
```

## Query Example

```javascript
db.average_rating.find().pretty()
```

---

# Data Visualizations

The following visualizations were created using Matplotlib:

### Top 10 Most Active Users

```text
top_active_users.png
```

### Occupation Distribution of Users Below 20 Years Old

```text
occupation_horizontal_bar_chart.png
```

### Distribution of Favourite Genres Among Active Users

```text
favourite_genre_distribution.png
```

### Gender Distribution of Scientists Aged 30–40

```text
scientist_gender_genre_piecharts.png
```

---

# Comparative Analysis

| Feature | Cassandra | HBase | MongoDB |
|----------|------------|------------|------------|
| Data Model | Wide Column | Wide Column | Document |
| Query Language | CQL | HBase Shell API | MongoDB Query Language |
| Scalability | Excellent | Excellent | Very Good |
| Read Performance | High | High | High |
| Write Performance | High | Very High | High |
| Ease of Use | Easy | Moderate | Easy |

---

# Results and Findings

Key findings obtained from the analysis include:

- Drama was the most preferred genre among active users.
- Several highly active users rated more than 500 movies.
- Most young users were students.
- Male scientists represented the majority of users aged between 30 and 40.
- Movie rating behaviour varied significantly across different demographic groups.

---

# Conclusion

This project successfully demonstrated the integration of Apache Spark with multiple NoSQL databases for large-scale data analytics. Apache Spark efficiently processed the MovieLens dataset while Cassandra, HBase and MongoDB provided different approaches to storing analytical results. The project highlights the strengths of distributed computing and NoSQL technologies in handling and analyzing large datasets.

---

# Repository Structure

```text
MovieLens-BigData-Analytics/
│
├── README.md
├── notebooks/
│   └── P162795_Assignment2_MovieLens.json
│
├── screenshots/
│   ├── top_active_users.png
│   ├── occupation_horizontal_bar_chart.png
│   ├── favourite_genre_distribution.png
│   └── scientist_gender_genre_piecharts.png
│
└── reports
```

---

# Author

**Syakirah Aliah**

Master of Science (Data Science and Analytics)

National University of Malaysia

---

# Acknowledgement

MovieLens dataset provided by the GroupLens Research Project, University of Minnesota.
