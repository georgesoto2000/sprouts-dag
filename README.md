# sprouts-dag

DAG to upload data from a psuedo Sports Events API to BigQuery table, download sales data from a GCP bucket, upload it to a BigQuery table, copy it to an archive bucket, delete it from the original bucket, and trigger a DBT pipeline.

## Setup Repo and Venv
Run the following commands in your working directory:
* `git clone https://github.com/georgesoto2000/sprouts-dag.git`
* `cd sprouts-dag`
* `python -m venv venv`
* `source venv/bin/activate`
* `pip install -r requirements.txt`

## Running Locally
Run the following commands in your terminal (airflow 3):
* `airflow db migrate` to setup the local airflow database
* Change dags_folder to the full path to sprouts-dag/dag `export AIRFLOW__CORE__DAGS_FOLDER=dags_folder`
* `airflow scheduler` in one terminal
* Create a new user replacing firstname, lastname and email: 
`airflow users create \
    --username admin \
    --firstname firstname \
    --lastname lastname \
    --role Admin \
    --email email@email.com`
* This will return a password you should record
*  In a new terminal run: `export AIRFLOW__API_AUTH__JWT_SECRET=` adding a JWT secret password
*  `airflow api-server -p 8080` 
* Go to localhost:8080
* Login and run the dag from the UI


## Deploying in Cloud Composer

Details on deploying to cloud composer can be found here: https://cloud.google.com/composer/docs/composer-3/manage-dags. Alternatively, after setting up your cloud composer environment, you can deploy using github: https://medium.com/@amarachi.ogu/implementing-ci-cd-in-cloud-composer-using-cloud-build-and-github-part-1-f0a4240d9984

## my_sprouts_dag.py

This is the main DAG folder containing utils and dag file. On a high level the dag performs the following steps: 
* Get Sports Events data from API
* Upload to BQ
* Get sales data from GCS standard bucket
* Upload to BQ
* Upload sales to GCS archive bucket
* Trigger a DBT pipeline

## Utils

Contains all the utils used by the main DAG.

### GCPUtils

Contains GCSClient and BigQueryClient. These classes are reusable components which allow for:
* Reading data from storage buckets
* Deleting data from storage buckets
* Uploading data to storage buckets
* Upload data to a BigQuery table

### SportsEventsAPI

Used as a dummy for retrieving data from the Sports Events API. Returns manually created data