import googleapiclient.discovery
from datetime import datetime
import pandas as pd
import config
from googleapiclient.errors import HttpError


def _get_client():

    """
    :return: A dataproc handle
    """

    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc


def _calculate_cpu_cost(machine_type):

    """
    :param machine_type: A GCP machine type e.g "n1-standard-8"
    :return: Return the price for a particular machine type
    """

    charges = _get_price_list()
    return float(charges[(charges['type'] == machine_type)]['cost'].values[0][1:]) / 60.00


def _get_master_cost(cluster, job_duration):

    """
    :param cluster: the cluster handle
    :param job_duration: teh duration of the job
    :return: total cost of the master node
    """

    master_type = cluster['config']['masterConfig']['machineTypeUri'].split('/')[-1]
    master_instances = cluster['config']['masterConfig']['numInstances']
    master_instance_cost = _calculate_cpu_cost(master_type)
    total_master_cost = master_instances * master_instance_cost * job_duration
    return total_master_cost


def _get_worker_cost(cluster, job_duration):

    """
    :param cluster: the cluster handle
    :param job_duration: job duration
    :return: total cost of worker nodes for the job
    """

    worker_type = cluster['config']['workerConfig']['machineTypeUri'].split('/')[-1]
    worker_instances = cluster['config']['workerConfig']['numInstances']
    worker_instance_cost = _calculate_cpu_cost(worker_type)
    total_worker_cost = worker_instances * worker_instance_cost * job_duration
    return total_worker_cost


def _get_operation_cost(dataproc, project, region, job_duration):

    """
    :param dataproc: the dataproc handle
    :param project: the project_id
    :param region: the geographic region of the project
    :param job_duration: the duration of the job
    :return: total cost of the job
    """

    # @TODO: include bootdisk space cost as well
    # As of now just get a list and sets the current instance
    # But can be configured to send the cluster name from airflow

    result = dataproc.projects().regions().clusters().list(
        projectId=project,
        region=region).execute()

    # The dataproc cluster instance, consider only the first one
    cluster = result['clusters'][0]
    total_master_cost = _get_master_cost(cluster, job_duration)
    total_worker_cost = _get_worker_cost(cluster, job_duration)
    total_cost = total_master_cost + total_worker_cost
    return total_cost


def _get_job_duration(dataproc, project, region, job_id):

    """
    :param dataproc: the dataproc handle
    :param project: the project_id
    :param region: the geographical region
    :param job_id: the dataproc job_id
    :return: the job_duration  in minutes
    """

    result = dataproc.projects().regions().jobs().get(
        projectId=project,
        region=region,
        jobId=job_id).execute()

    start_time = None
    end_time = None

    status = result['status']
    if status['state'] == 'DONE':
        end_time = status['stateStartTime']
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S.%fZ')

    status_history = result['statusHistory']

    for status in status_history:
        if status['state'] == 'RUNNING':
            start_time = status['stateStartTime']
            start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        if status['state'] == 'ERROR':
            raise Exception(status['details'])
        elif status['state'] == 'DONE':
            end_time = result['status']['stateStartTime']

    job_duration = float((end_time-start_time).seconds)/float(60)

    # If it's less than 10 minutes, return 10 minutes as it's the min duration
    if job_duration < 10.00:
        job_duration = 10.00
    return job_duration


def _get_price_list():

    """
    :return: the price list from a CSV file
    """
    charges = pd.read_csv(config.PRICE_LIST_CSV, header=0, sep='\t')
    return charges


def _get_job_id(dataproc, project_id, region):

    """
    :param dataproc: the dataproc cluster handle
    :param project_id: the project_id
    :param region:  the dataproc region
    :return: the job id of the last job with a valid status
    """
    job_id = dataproc.projects().regions().jobs().list(
        projectId=project_id,
        region=region
    ).execute()['jobs'][0]['reference']['jobId']
    return job_id


def get_cost_for_last_job():

    """

    :return: The last dataproc job cost
    """

    project_id = config.PROJECT_ID
    region = config.PROJECT_REGION
    dataproc =  None
    try:
        print "trying to get a dataproc instance..."
        dataproc = _get_client()
    except Exception, e:
        print "Check gcloud config and project access: {}".format(e)

    if dataproc is not None:
        job_id = _get_job_id(dataproc, project_id, region)
        job_duration = _get_job_duration(dataproc, project_id, region, job_id)
        cluster_cost = _get_operation_cost(dataproc, project_id, region, job_duration)
        print "total operation cost: $",cluster_cost
        return job_duration, cluster_cost

