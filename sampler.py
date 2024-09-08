#!/usr/bin/env python

import argparse
import os
import re
import json
from collections import defaultdict
from datetime import datetime, timezone
from kubernetes import client, config
import subprocess
import psycopg2
import logging

from settings import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

RUNAI_GROUP = 'run.ai'
RUNAI_VERSION = 'v2alpha1'


def k8s_setup():
    config.load_kube_config()
    k8s = client.CoreV1Api()

    return k8s


def get_pods(k8s, namespace):
    try:
        pods = k8s.list_namespaced_pod(namespace).items
    except Exception as e:
        logging.error(f"list_namespaced_pod failed: {e}")

    return pods


def get_events_for_namespace(namespace):
    api_instance = client.CoreV1Api()
    events = api_instance.list_namespaced_event(namespace)

    return events.items


def get_resources_by_type(resource_type, namespace, crd_group='', version='v1'):
    resources = {}
    try:
        custom_api = client.CustomObjectsApi()
        resources = custom_api.list_namespaced_custom_object(group=crd_group, version=version, namespace=namespace, plural=resource_type)
    except Exception as e:
        logging.error(f"list_namespaced_custom_object failed: {e}")

    return resources['items']


def get_runai_resources_by_type(resource_type, namespace):
    return get_resources_by_type(resource_type, namespace, RUNAI_GROUP, RUNAI_VERSION)


def get_workload_key_from_workload(resource):
    # resource['metadata']['name'] is not the name we want, as in old runai-cli
    # date is added to the workload name. we need the short name
    workload_name = resource['spec']['name']['value']
    workload_namespace = resource['metadata']['namespace']

    return workload_name, workload_namespace


def get_workload_key_from_job(resource):
    workload_name = resource['metadata']['name']
    workload_namespace = resource['metadata']['namespace']

    return workload_name, workload_namespace


def get_workload_key_from_pod(pod):
    try:
        job_name = pod.metadata.labels['release']  # training case
    except KeyError:
        job_name = pod.metadata.labels['training.kubeflow.org/job-name']  # distributed case

    return job_name, pod.metadata.namespace


def get_workload_key_from_podgroup(podgroup):
    return podgroup['metadata']['labels']['release'], podgroup['metadata']['namespace']


def get_required_resources_from_cluster(k8s, workload_type, namespace):
    # get all the required resources from the cluster
    workload_crd_plural = f"{workload_type}workloads"
    logging.info(f'getting {workload_crd_plural}')
    workloads = get_runai_resources_by_type(workload_crd_plural, namespace)

    if workload_type == "distributed":
        logging.info('getting pytorchjobs')
        jobs = get_resources_by_type('pytorchjobs', namespace, 'kubeflow.org')
    else:
        logging.info('getting runaijobs')
        jobs = get_resources_by_type('runaijobs', namespace, 'run.ai')

    logging.info('getting pods')
    pods = get_pods(k8s, namespace)

    logging.info('getting podgroups')
    podgroups = get_resources_by_type('podgroups', namespace, 'scheduling.run.ai')

    if TEST_SCHEDULER_EVENT_TIMES:
        logging.info('getting events')
        events = get_events_for_namespace(namespace)
    else:
        events = {}

    return workloads, jobs, pods, podgroups, events


def connect_to_database():
    if IS_SELF_HOSTED_DB:
        db_params = {
            'dbname': SELF_HOSTED_DB_NAME,
            'user': SELF_HOSTED_DB_USER,
            'password': SELF_HOSTED_DB_PASSWORD,
            'host': 'localhost',
            'port': 5432
        }
    else:
        cmd = f"aws rds generate-db-auth-token \
                --hostname {STAGING_DB_HOST} \
                --port 5432 \
                --region {STAGING_DB_REGION} \
                --username {STAGING_DB_USER}"
        result = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        db_password = result.stdout.strip()

        db_params = {
            'dbname': STAGING_DB_NAME,
            'user': STAGING_DB_USER,
            'password': db_password,
            'host': STAGING_DB_HOST,
            'port': 5432
        }

    return psycopg2.connect(**db_params)


def get_required_data_from_backend(workload_type, namespace):
    # read job info from the jobs table in the control-plane

    project = namespace.replace('runai-', '')

    kind_operator = '='
    if workload_type == 'distributed':
        kind_operator = '!='

    query = f"""
        SELECT name, time_created
        FROM jobs
        WHERE project = '{project}'
        AND type = 'Train'
        AND kind {kind_operator} 'RunaiJob'
        AND exists_in_cluster = 'true'
    """

    if not IS_SELF_HOSTED_DB:
        query += '\n' + f"AND cluster_uuid = '{STAGING_RUNAI_CLUSTER_UUID}'"

    try:
        logging.info('getting backend jobs')
        connection = connect_to_database()
        cursor = connection.cursor()
        cursor.execute(query)

        rows = cursor.fetchall()

        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        return []

    backend_jobs = []
    for row in rows:
        job_name = row[0]
        timestamp = row[1]/1000
        job = {"jobName": job_name,
               "projectName": project,
               "jobNamespace": namespace,
               "backendJobCreatedTimestamp": datetime.fromtimestamp(timestamp, timezone.utc)
        }

        backend_jobs.append(job)

    return backend_jobs


def join_data_by_workload(workloads, jobs, pods, podgroups, events, backend_jobs):
    # join all the data into a single dictionary
    # key = (workload_name, namespace)
    # value = dict with objects (e.g. workload, pod)
    data = defaultdict(dict)

    pgEvictions, pgPvcBindRequests, pgPvcBinds = sort_events_by_podgroup(events, podgroups)

    for workload in workloads:
        k = get_workload_key_from_workload(workload)
        data[k]['workload'] = workload

    for job in jobs:
        k = get_workload_key_from_job(job)
        data[k]['job'] = job

    for pod in pods:
        k = get_workload_key_from_pod(pod)
        if 'pods' not in data[k]:
            data[k]['pods'] = []
        data[k]['pods'].append(pod)

    for podgroup in podgroups:
        k = get_workload_key_from_podgroup(podgroup)
        data[k]['podgroup'] = podgroup
        pgName = podgroup['metadata']['name']

        if pgName in pgEvictions:
            data[k]['evictionTimes'] = pgEvictions[pgName]
        else:
            data[k]['evictionTimes'] = []

        if pgName in pgPvcBindRequests:
            data[k]['pvcBindRequestTimes'] = pgPvcBindRequests[pgName]
        else:
            data[k]['pvcBindRequestTimes'] = []

        if pgName in pgPvcBinds:
            data[k]['pvcBindTimes'] = pgPvcBinds[pgName]
        else:
            data[k]['pvcBindTimes'] = []

    for backend_job in backend_jobs:
        k = (backend_job['jobName'], backend_job['jobNamespace'])
        data[k]['backend_job'] = backend_job

    return data


def sort_events_by_podgroup(events, pod_groups):
    evictions_by_pg = {}
    pvc_bind_requests_by_pg = {}
    pvc_binds_by_pg = {}

    for event in events:
        if event.reason == 'Evict':
            if "preempted" in event.message:
                pod, pg, time = extract_preemption_data(event)
                if pg not in evictions_by_pg:
                    evictions_by_pg[pg] = []
                evictions_by_pg[pg].append(event.first_timestamp)
            elif "reclaim" in event.message:
                pod, pg, time = extract_reclaim_data(event)
                if pg not in evictions_by_pg:
                    evictions_by_pg[pg] = []
                evictions_by_pg[pg].append(event.first_timestamp)
        elif event.reason == 'ExternalProvisioning':
            pg, time = extract_pvc_bind_request_data(event, pod_groups)
            if pg not in evictions_by_pg:
                pvc_bind_requests_by_pg[pg] = []
            pvc_bind_requests_by_pg[pg].append(event.first_timestamp)
        elif event.reason == 'ProvisioningSucceeded':
            pg, time = extract_pvc_bind_data(event, pod_groups)
            if pg not in evictions_by_pg:
                pvc_binds_by_pg[pg] = []
            pvc_binds_by_pg[pg].append(event.first_timestamp)

    return evictions_by_pg, pvc_bind_requests_by_pg, pvc_binds_by_pg


def extract_preemption_data(event):
    preempteePattern = "runai-.*?/.*? "
    preempteePod = re.findall(preempteePattern, event.message)[0].replace(" ", "").split('/')[1]

    preemptorPattern = "preempted by higher priority job runai-.*?/pg.*?$"
    preemptorPG = re.findall(preemptorPattern, event.message)[0].replace("preempted by higher priority job ", "").split('/')[1]

    time = event.first_timestamp

    return preempteePod, preemptorPG, time


def extract_reclaim_data(event):
    reclaimeePattern = "runai-.*?/.*? "
    reclaimeePod = re.findall(reclaimeePattern, event.message)[0].replace(" ", "").split('/')[1]

    reclaimerPattern = "reclaimed by job runai-.*?/pg.*?\."
    reclaimerPG = re.findall(reclaimerPattern, event.message)[0].replace("reclaimed by job .+ ", "").replace(".", "").split('/')[1]

    time = event.first_timestamp

    return reclaimeePod, reclaimerPG, time


def extract_pvc_bind_request_data(event, podgroups):
    pgPattern = "j-.{6}"
    workload = re.findall(pgPattern, event.involved_object.name)[0]
    time = event.first_timestamp

    pg = ""
    for podGroup in podgroups:
        if podGroup['metadata']['labels']['workloadName'] == workload:
            pg = podGroup['metadata']['name']

    return pg, time


def extract_pvc_bind_data(event, podgroups):
    pgPattern = "j-.{6}"
    workload = re.findall(pgPattern, event.involved_object.name)[0]
    time = event.first_timestamp

    pg = ""
    for podGroup in podgroups:
        if podGroup['metadata']['labels']['workloadName'] == workload:
            pg = podGroup['metadata']['name']

    return pg, time


def get_min_max_pod_times(pods):
    pod_times = [pod.metadata.creation_timestamp for pod in pods]
    return min(pod_times), max(pod_times)


def get_pod_scheduling_decision_time(pod):
    # returns the last transition time of the PodScheduled condition of the pod,
    # if the condition is either true (i.e. scheduled) or false with reason.
    # otherwise returns None
    try:
        if "status" in pod.to_dict() and "conditions" in pod.status.to_dict() and pod.status.conditions:
            for condition in pod.status.conditions:
                if condition.type == "PodScheduled" and (condition.status == "True" or condition.reason):
                    return condition.last_transition_time

    except AttributeError:
        pass

    raise KeyError


def extract_times_from_resources(data):
    times = []

    for workload_key, workload_resources in data.items():
        workload_name, namespace = workload_key

        workload_times = {}
        workload_times['jobName'] = workload_name
        workload_times['jobNamespace'] = namespace
        workload_times['projectName'] = namespace.replace('runai-', '')

        if 'workload' not in workload_resources:
            # that means we found some resource (e.g. pod) but its parent workload
            # no longer exists. it should not be part of our result
            continue

        try:
            first_pod_timestamp, last_pod_timestamp = get_min_max_pod_times(workload_resources['pods'])
            workload_times['workloadCreatedTimestamp'] = workload_resources['workload']['metadata']['creationTimestamp']
            workload_times['jobCreatedTimestamp'] = workload_resources['job']['metadata']['creationTimestamp']
            workload_times['firstPodCreatedTimestamp'] = first_pod_timestamp.isoformat()
            workload_times['lastPodCreatedTimestamp'] = last_pod_timestamp.isoformat()
            workload_times['podGroupCreatedTimestamp'] = workload_resources['podgroup']['metadata']['creationTimestamp']
            workload_times['podSchedulingDecisionTimestamp'] = get_pod_scheduling_decision_time(workload_resources['pods'][0]).isoformat()

            workload_times['firstEvictionTimestamp'] = workload_times['podSchedulingDecisionTimestamp']
            if workload_resources['evictionTimes']:
                workload_times['firstEvictionTimestamp'] = min(workload_resources['evictionTimes']).isoformat()

            workload_times['firstPVCBindRequestTimestamp'] = workload_times['podSchedulingDecisionTimestamp']
            if workload_resources['pvcBindRequestTimes']:
                workload_times['firstPVCBindRequestTimestamp'] = min(workload_resources['pvcBindRequestTimes']).isoformat()

            workload_times['firstPVCBindTimestamp'] = workload_times['podSchedulingDecisionTimestamp']
            if workload_resources['pvcBindTimes']:
                workload_times['firstPVCBindTimestamp'] = min(workload_resources['pvcBindTimes']).isoformat()

            if TEST_BACKEND_TIMES:
                workload_times['backendJobCreatedTimestamp'] = workload_resources['backend_job']['backendJobCreatedTimestamp'].isoformat()
            else:
                workload_times['backendJobCreatedTimestamp'] = workload_times['workloadCreatedTimestamp']

        except KeyError as e:
            logging.warning(f"some resources are not ready yet, workloads are still being handled. Workload {workload_name}/{namespace}, error: {e} (skipping)")
            continue

        times.append(workload_times)

    return times


def sample_workloads(k8s, workload_type, namespace):
    workloads, jobs, pods, podgroups, events = get_required_resources_from_cluster(k8s, workload_type, namespace)
    if TEST_BACKEND_TIMES:
        backend_jobs = get_required_data_from_backend(workload_type, namespace)
    else:
        backend_jobs = []

    print()
    logging.info("resources:")
    logging.info(f" num workloads: {len(workloads)}")
    logging.info(f" num jobs: {len(jobs)}")
    logging.info(f" num pods: {len(pods)}")
    logging.info(f" num podgroups: {len(podgroups)}")
    logging.info(f" num events: {len(events)}")
    logging.info(f" num backend jobs: {len(backend_jobs)}")
    print()

    data = join_data_by_workload(workloads, jobs, pods, podgroups, events, backend_jobs)
    times = extract_times_from_resources(data)

    return times


def write_json(output_dir, times):
    file_path = f"{output_dir}/sampled.json"

    try:
        with open(file_path, "w") as file:
            json.dump(times, file)
    except Exception as e:
        logging.error(f"Failed writing to {file_path}: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', '-o', type=str, default=DEFAULT_OUTPUT_DIR, help='Output dir')
    parser.add_argument('--workload-type', '-t', choices=['training', 'distributed', 'interactive'], default='training', help='Workload type (default: training)')
    parser.add_argument('--project', '-p', type=str, default=DEFAULT_PROJECT, help='Project to sample')

    args = parser.parse_args()

    if not os.path.isdir(args.output_dir):
        logging.error(f"output dir '{args.output_dir}' doesn't exist")
        exit(1)

    if TEST_BACKEND_TIMES:
        try:
            connection = connect_to_database()
            connection.close()
        except Exception as e:
            logging.error(f"failed to connect to database, check your vpn connection and credentials: {e}")
            exit(1)

    namespace = f'runai-{args.project}'

    k8s = k8s_setup()
    times = sample_workloads(k8s, args.workload_type, namespace)
    write_json(args.output_dir, times)

    logging.info(f"generated time info for {len(times)} workloads")
    if False:
        for t in times:
            print(t)
