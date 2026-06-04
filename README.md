# PySpark Finance Data Analysis Project

A financial customer data analysis project built with **Apache Spark** and **PySpark** for distributed, large-scale data processing, feature engineering, and exploration.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [What is Apache Spark?](#what-is-apache-spark)
3. [What is PySpark?](#what-is-pyspark)
4. [How PySpark Works in This Project](#how-pyspark-works-in-this-project)
5. [Why Use PySpark?](#why-use-pyspark)
6. [PySpark vs Traditional Pandas](#pyspark-vs-traditional-pandas)
7. [Technical Architecture](#technical-architecture)
8. [Project Features](#project-features)
9. [Setup & Usage](#setup--usage)

---

## Project Overview

This project analyzes customer banking data to understand customer behavior and churn patterns using distributed computing. It processes customer financial information including:

- **Customer Demographics**: Age, gender, geography
- **Financial Metrics**: Credit score, balance, estimated salary
- **Account Activity**: Tenure, number of products, card status, active member status
- **Target Variable**: Customer exit/churn status

The project demonstrates:

- Loading and exploring large datasets efficiently
- Feature engineering using User Defined Functions (UDFs)
- Distributed aggregations and grouping operations
- SQL queries on distributed data
- Scalable data transformation pipelines

---

## What is Apache Spark?

### Definition

**Apache Spark** is a unified, open-source distributed computing framework designed for fast, large-scale data processing across clusters of computers.

### Key Characteristics

| Feature                 | Description                                                   |
| ----------------------- | ------------------------------------------------------------- |
| **Speed**               | Processes data in-memory, 100x faster than MapReduce          |
| **Distributed**         | Runs on clusters, splitting computation across multiple nodes |
| **In-Memory Computing** | Uses RAM for intermediate results, minimizing disk I/O        |
| **Fault-Tolerant**      | Recovers from node failures automatically                     |
| **Lazy Evaluation**     | Executes operations only when an action is requested          |
| **Multi-Language**      | Supports Scala, Java, Python (PySpark), R (SparkR), SQL       |

### Core Components

1. **Spark Core**: Base engine for distributed task scheduling and memory management
2. **Spark SQL**: Structured data processing with SQL and DataFrames
3. **Spark Streaming**: Real-time stream processing
4. **MLlib**: Machine learning library for distributed algorithms
5. **GraphX**: Graph computation framework

### Architecture

```
┌─────────────────────────────────────────┐
│       Spark Application (Driver)        │
├─────────────────────────────────────────┤
│   Distributed Processing (Executors)    │
│  ┌──────────┐  ┌──────────┐  ┌────────┐│
│  │Executor 1│  │Executor 2│  │Executor││
│  │  Tasks   │  │  Tasks   │  │ Tasks  ││
│  └──────────┘  └──────────┘  └────────┘│
└─────────────────────────────────────────┘
```

---

## What is PySpark?

### Definition

**PySpark** is the Python API for Apache Spark. It enables Python developers to interact with Spark's distributed computing engine using familiar Python syntax.

### How It Works

- **Python Bindings**: PySpark communicates with Spark's Java/Scala core through the Py4J bridge
- **Python Objects → JVM**: PySpark serializes Python objects and sends them to the Java Virtual Machine (JVM) for execution
- **Lazy Execution**: Operations are not executed immediately; they're queued until an action is called
- **API Consistency**: Provides a Pythonic interface similar to pandas while leveraging distributed computing

### PySpark Ecosystem

```
PySpark User Code (Python)
        ↓ (Py4J)
Spark Driver (JVM)
        ↓
Spark Executors (JVM - Multiple Nodes)
        ↓
Distributed Data Processing
```

---

## How PySpark Works in This Project

### 1. **SparkSession Creation**

```python
spark = SparkSession.builder \
    .appName("PySpark Finance") \
    .master("local[*]") \
    .getOrCreate()
```

- Creates a Spark session, the entry point for all Spark operations
- `local[*]` runs on local machine using all available CPU cores
- `.getOrCreate()` reuses existing session if available

### 2. **Data Loading**

```python
data = spark.read.csv("train.csv", header=True, inferSchema=True)
```

- Reads CSV file into a **Spark DataFrame** (distributed table)
- `inferSchema=True` automatically detects column data types
- Data is partitioned across executors for parallel processing

### 3. **Data Exploration**

```python
data.show(5)  # Display first 5 rows
data.printSchema()  # Show column types and structure
```

- **Lazy evaluation**: `show()` is an action that triggers actual computation
- Spark retrieves only the first partition needed for display

### 4. **Distributed Operations**

```python
# Filtering across all data nodes
data.where(data["Exited"] == 1).groupBy("Geography").count()
```

- Spark **partitions** the data and filters in parallel
- **GroupBy** aggregates results from all partitions
- Results are combined at the driver node

### 5. **User Defined Functions (UDFs)**

```python
def GeoGender(geo, gender):
    return f"{geo}_{gender}"

geo_gender_udf = udf(GeoGender, StringType())
data = data.withColumn("GeoGender", geo_gender_udf(col("Geography"), col("Gender")))
```

- UDFs extend Spark's built-in functions with custom logic
- Applied across all rows in parallel
- Return type must be specified for optimization

### 6. **SQL Queries on Distributed Data**

```python
data_updated.createOrReplaceTempView("customer_data")
spark.sql("""
    SELECT GeoGender, EstimatedSalary, TotalProductsUsed,
           CardActiveMember, Exited
    FROM customer_data
    LIMIT 10
""").show()
```

- Creates temporary table for SQL queries
- Spark SQL optimizer converts SQL to distributed operations
- Combines benefits of SQL and distributed computing

---

## Why Use PySpark?

### 1. **Scalability**

- **Horizontal Scaling**: Add more machines, not CPU power
- **Big Data**: Process datasets larger than RAM
- **No Code Changes**: Same code runs on local machine or 1000-node cluster

### 2. **Performance**

- **100x Faster**: In-memory processing vs. MapReduce on disk
- **Parallel Execution**: Processes data across multiple cores/nodes simultaneously
- **Optimized Queries**: Catalyst optimizer automatically optimizes execution plans

### 3. **Distributed Computing Abstraction**

- **Simple API**: Write code as if data fits in memory
- **Handles Complexity**: Automatic task distribution, fault tolerance, data shuffling
- **No Manual Coordination**: Spark manages parallelization

### 4. **Multi-Paradigm Support**

- **Batch Processing**: Historical data analysis
- **Streaming**: Real-time data processing
- **ML**: Built-in machine learning library
- **Graph Processing**: Network analysis
- **SQL**: Familiar SQL interface

### 5. **Fault Tolerance**

- **RDD Lineage**: Tracks data transformations for recovery
- **Node Failure**: Automatically re-computes lost partitions
- **Data Reliability**: No data loss even if nodes fail

### 6. **Language Flexibility**

- **Python API**: Leverage Python libraries, familiar syntax
- **Integration**: Works with NumPy, Pandas, Scikit-learn
- **Familiar Tools**: Use Jupyter notebooks like in this project

---

## PySpark vs Traditional Pandas

| Aspect              | **Pandas**                   | **PySpark**                        |
| ------------------- | ---------------------------- | ---------------------------------- |
| **Data Size**       | Single Machine (RAM limited) | Distributed Clusters (TB/PB scale) |
| **Processing**      | Sequential                   | Parallel across nodes              |
| **Typical Dataset** | < 10 GB                      | > 100 GB                           |
| **Speed**           | Fast for small data          | 10-100x faster for large data      |
| **Memory**          | All data in RAM              | Out-of-core processing             |
| **Computation**     | Eager evaluation             | Lazy evaluation (optimized)        |
| **Setup**           | `pip install pandas`         | Requires Spark cluster setup       |
| **Learning Curve**  | Easy for beginners           | Steeper (distributed concepts)     |
| **Use Cases**       | EDA, small analytics         | Big data, production pipelines     |
| **Fault Tolerance** | None                         | Automatic (RDD lineage)            |
| **Cost**            | Low                          | Higher (cluster infrastructure)    |

### Example Comparison

**Pandas (Single Machine)**

```python
import pandas as pd
df = pd.read_csv("large_file.csv")  # Must load entire file into RAM
result = df.groupby("Geography").size()  # Processes on single core
```

**PySpark (Distributed)**

```python
spark = SparkSession.builder.master("cluster").getOrCreate()
df = spark.read.csv("large_file.csv")  # Reads partitions in parallel
result = df.groupBy("Geography").count()  # Processes across all cores/nodes
```

### When to Use Each

**Use Pandas When:**

- Data fits in RAM
- Quick exploratory analysis needed
- Single machine / laptop
- Small team project
- Simplicity is priority

**Use PySpark When:**

- Data > 10 GB
- Need real-time processing at scale
- Working with distributed systems
- Production ML pipelines
- Team needs fault tolerance

---

## Technical Architecture

### Spark Execution Flow (DAG)

```
RDD 1 (Load Data)
    ↓
RDD 2 (Filter)
    ↓
RDD 3 (Transform)
    ↓
RDD 4 (GroupBy)
    ↓ [ACTION - Execution Triggered]
Result
```

### Lazy Evaluation Benefits

```python
# These don't execute:
df = spark.read.csv("train.csv")      # Transformation
df_filtered = df.where("Exited == 1")  # Transformation
df_grouped = df_filtered.groupBy("Geography").count()  # Transformation

# This EXECUTES everything above:
df_grouped.show()  # Action - triggers full execution
```

### Partitioning Strategy

- **Default**: Spark auto-partitions based on file size
- **Custom**: `.repartition(n)` increases/decreases partitions
- **Broadcast**: Small DataFrames broadcast to all nodes (efficient joins)

### DataFrames vs RDDs

| Feature          | RDD                            | DataFrame                     |
| ---------------- | ------------------------------ | ----------------------------- |
| **Type**         | Generic distributed collection | Distributed table with schema |
| **Optimization** | None (user responsible)        | Catalyst optimizer            |
| **Performance**  | Slower                         | 10-100x faster                |
| **SQL Support**  | No                             | Yes                           |
| **Type Safety**  | At runtime                     | Compile-time (Scala)          |
| **Use Case**     | Unstructured data              | Structured/semi-structured    |

---

## Project Features

### 1. **Data Loading & Exploration**

- Load CSV data with automatic schema inference
- Display sample rows and schema
- Compute data statistics

### 2. **Filtering & Selection**

- Filter rows based on conditions (e.g., `Exited == 1`)
- Select specific columns
- Explore customer churn patterns

### 3. **Aggregation & Grouping**

- Group by geography: Count churned vs. retained customers
- Analyze patterns by region
- Distributed aggregation across partitions

### 4. **Feature Engineering with UDFs**

Four custom features created:

| Feature               | Description                       | Type    |
| --------------------- | --------------------------------- | ------- |
| **GeoGender**         | Combines geography and gender     | String  |
| **BE_Ratio**          | Balance to Estimated Salary ratio | Float   |
| **TotalProductsUsed** | Products × Tenure interaction     | Integer |
| **CardActiveMember**  | Card status × Active status       | String  |

### 5. **SQL Queries**

- Create temporary views of distributed data
- Query using standard SQL syntax
- Combine with Spark transformations

---

## Setup & Usage

### Prerequisites

```bash
# Python 3.8+
# Java 8 or 11 (Spark requires JVM)
# pip install pyspark
```

### Installation

```bash
pip install pyspark pandas jupyter
```

### Running the Notebook

1. **Start Jupyter**

   ```bash
   jupyter notebook pyspark_data.ipynb
   ```

2. **Execute Cells**
   - Run all cells to process customer data
   - View transformations and aggregations

### Expected Output

```
Spark Version: 4.1.2

+---+----------+--------------+-----------+---------+------+----+------+---------+
| id|CustomerId|      Surname  |CreditScore|Geography|Gender| Age|Tenure|  Balance|
+---+----------+--------------+-----------+---------+------+----+------+---------+
|  0|  15674932|Okwudilichukwu|        668|   France|  Male| 33 |     3|      0.0|
```

### Scaling to Cluster

```python
# Change from local to cluster:
spark = SparkSession.builder \
    .appName("PySpark Finance") \
    .master("spark://cluster-master:7077")  # Cluster URL
    .getOrCreate()
```


