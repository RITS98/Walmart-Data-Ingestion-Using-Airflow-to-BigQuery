# Walmart Data Ingestion Using Airflow to GCP BigQuery

This project is on how to ingest data using Apache Airflow (Managed Apache Airflow on GCP Composer) to GCP BigQuery. It involves creating buckets to store the data, creating GCP Composer, and creating tables in BigQuery
all automatically using managed Apache Airflow.

## Architecture

```mermaid flowchart LR A[GCP Buckets<br>walmart_data/] --> B[Airflow DAG<br>GCP Cloud Composer] B --> C1[BigQuery Table<br>walmart_dwh.merchants_data] B --> C2[BigQuery Table<br>walmart_dwh.sales_data] B --> C3[BigQuery Table<br>walmart_dwh.target_data] style A fill:#f7f7f7,stroke:#555,stroke-width:1px style B fill:#e6f7ff,stroke:#2b7a78,stroke-width:1.5px style C1 fill:#fffbe6,stroke:#c59c00,stroke-width:1px style C2 fill:#fffbe6,stroke:#c59c00,stroke-width:1px style C3 fill:#d3f9d8,stroke:#2f8132,stroke-width:2px ```

## Steps

### Create buckets in GCP

1. Create a bucket by clicking on `Create` button. Choose region and other settings like replication, etc., as per requirements

<img width="1260" alt="image" src="https://github.com/user-attachments/assets/67c3dbd7-7dd4-4af7-822b-e674983151c9" />


2. Created two folders `merchants` and `sales` to store the json data.

<img width="1243" alt="image" src="https://github.com/user-attachments/assets/e582e96a-66fe-4fc7-9292-0777d10df308" />

 ### Create Airflow Instance in GCP Composer

 1. When creating GCP Composer for the first time, we need to enable GCP COmposer API.
 2. Click on `Create Environment` button. Choose the modern option.
 3. Create a Airflow Environment using the below options. Change it as per usage. I have opted for small for this project.

<img width="1118" alt="image" src="https://github.com/user-attachments/assets/fd70bf5b-d875-4068-80e4-a22f2e74f6ec" />

<img width="957" alt="image" src="https://github.com/user-attachments/assets/55a4b857-7b6c-478f-ad5c-f928ee50a45e" />


 4. Click on `Create` button.

 5. It will take some time to create the Airflow instance.
 6. Upload the day python file by clickin on the `DAGs` link and then `OPEN DAGS FOLDER` option.

<img width="1541" alt="image" src="https://github.com/user-attachments/assets/b525d53b-6156-4849-903e-15f9c42727da" />

<img width="1668" alt="image" src="https://github.com/user-attachments/assets/7932b4a1-73d6-45fa-80cf-66ad4e7e0254" />

<img width="1472" alt="image" src="https://github.com/user-attachments/assets/d29f4a11-3874-4867-8fb9-7c9f1c159fc5" />

## Results




