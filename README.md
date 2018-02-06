## google-dataproc-job-cost-estimator

 `google-dataproc-job-cost-estimator` is a basic Python script that determines the cost of Hadoop/Spark jobs run on a **Google Cloud Platform Dataproc** instance. This is a naive script that doesn't account for multi-tenant applications. I have used this script successfully with my **Airflow** tasks.
  
## Requirements

You should have installed the requirements.txt and then initialised your `gcloud` credentials. This application relies on the default gcloud credentials

## Usage

To get the cost of the last job with a valid status on Dataproc simply call `get_cost_for_last_job()` from within your application