## ETL-Ecommerce-data
### Objective
Build an end-to-end ETL pipeline for Brazilian E-Commerce Public Dataset by Olist with 
<br> Medallion Architecture pattern using Docker, Spark, Postgres, Astro cli and Grafana.
### System Architecture
![Untitled Diagram drawio (1)](https://github.com/user-attachments/assets/4e56d9ff-288a-437d-aa5d-c1da649d9809)

### Dataset
##### Brazilian E-Commerce Public Dataset by Olist [kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
![schema](https://github.com/user-attachments/assets/83204012-8601-42fd-8583-2cc8a1fe41c8)

### Medallion Architecture
1- Ingestion: The data is ingests from Kaggle using python code.
<br>2- Bronze layer: raw data stored in Bronze dataset in poatgres
<br>3- Silver layer: process the data from the bronze and store it in silver layer.
<br>4- Gold: create analytics tables using data from the silver for visualization and and store it in gold layer.

### Visualization
1- Connect Grafana with Gold dataset in postgres.
![vis](https://github.com/user-attachments/assets/c0447dbf-8a4e-472e-9600-cf06bc0eb4a7)



