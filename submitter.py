#!/usr/bin/env python

import argparse
import os
import sys
import json
import subprocess
import uuid
from timeout_decorator import timeout, TimeoutError
from datetime import datetime, timezone
import multiprocessing as mp
import time
import logging

from settings import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def write_json(output_dir, times):
    file_path = f"{output_dir}/submitted.json"

    try:
        with open(file_path, "w") as file:
            json.dump(times, file)
    except Exception as e:
        logging.error(f"Failed writing to {file_path}: {str(e)}")
        raise e


def generate_job_name():
    job_id = str(uuid.uuid4().hex)[:6]
    return f"j-{job_id}"


@timeout(60)
def submit_single_workload(job_name, workload_type, project, num_gpus, num_workers, pvc):
    if SUBMIT_USING_KUBECTL:
        current_script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        yaml_file_path = f"{current_script_dir}/yaml/{workload_type}.yaml"
        yaml_content = open(yaml_file_path, 'r').read()
        yaml_content = yaml_content.replace('JOB_NAME_PLACEHOLDER', job_name)
        yaml_content = yaml_content.replace('PROJECT_PLACEHOLDER', project)
        yaml_content = yaml_content.replace('NUM_WORKERS_PLACEHOLDER', num_workers)
        yaml_content = yaml_content.replace('NUM_GPUS', num_gpus)

        if pvc:
            pvc_yaml_file_path = f"{current_script_dir}/yaml/pvc.yaml"
            pvc_yaml_content = open(pvc_yaml_file_path, 'r').read()
            yaml_content = yaml_content.replace('status:', pvc_yaml_content+'status:')

        subprocess.run(['kubectl', 'apply', '-f', '-'], input=yaml_content, text=True, check=True)
    else:
        pvc_flag = ''
        if pvc:
            pvc_flag = ' --new-pvc ephemeral,size=1Mi,accessmode-rwo,path=/path-new,storageclass=openebs-lvmpv'

        if workload_type == "training":
           cmd = f"{RUNAI_CLI_PATH} submit {job_name} --project {project} -i gcr.io/run-ai-lab/ubuntu:loop --image-pull-policy IfNotPresent -g {num_gpus} {pvc_flag} --command -- sleep infinity"
        elif workload_type == "distributed":
           cmd = f"{RUNAI_CLI_PATH} submit-pytorch {job_name} --project {project} -i gcr.io/run-ai-lab/ubuntu:loop --image-pull-policy IfNotPresent --clean-pod-policy none -g {num_gpus} --workers 7 {pvc_flag} --command -- sleep infinity"
        else:
            raise

        subprocess.run(cmd, shell=True, check=True)


def submit_workload(workload_type, job_index, num_workloads, project, num_gpus, num_workers, pvc):
    job_name = generate_job_name()
    logging.info(f"submitting {workload_type} job {job_name} ({job_index+1}/{num_workloads})")
    try:
        submit_timestamp = datetime.now(timezone.utc).isoformat()
        submit_single_workload(job_name, workload_type, project, num_gpus, num_workers, pvc)
    except TimeoutError:
        logging.error(f"Timeout occurred for {job_name}. Skipping to the next workload.")
        return None
    except subprocess.CalledProcessError:
        logging.error(f"Failed to submit {job_name}. Skipping to the next workload.")
        return None

    s = {
        "jobName": job_name,
        "projectName": project,
        "jobNamespace": 'runai-' + project,
        "submitTimestamp": submit_timestamp
    }

    return s


def submit_workloads(output_dir, workload_type, num_workloads, num_processes, delay_seconds, project, num_gpus, num_workers, pvc):
    submission_data = []

    num_iterations = num_workloads // num_processes
    num_workloads = num_processes * num_iterations

    for iteration in range(num_iterations):
        with mp.Pool(processes=num_processes) as pool:
            submit_params = [(workload_type, i + (iteration * num_processes), num_workloads, project, num_gpus, num_workers, pvc) for i in range(num_processes)]
            results = pool.starmap(submit_workload, submit_params)

        for result in results:
            if result is not None:
                submission_data.append(result)

        # write partial json every iteration if we run with parallel processes, or every 16 items if we run serially
        if num_processes > 1 or (iteration % 8 == 0):
            logging.info("writing partial json")
            write_json(output_dir, submission_data)

        time.sleep(delay_seconds)

    return submission_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', '-o', type=str, default=DEFAULT_OUTPUT_DIR, help='Output dir')
    parser.add_argument('--workload-type', '-t', choices=['training', 'distributed', 'interactive'], default='general', help='Workload type (default: training)')
    parser.add_argument('--num-workloads', '-n', type=int, help='Number of workloads to submit')
    parser.add_argument('--num-processes', '-p', type=int, default=8, help='Number of parallel processes to use for submission (default: 8)')
    parser.add_argument('--num-workers', type=str, default='1', help='Number of workers in a distributed workload (default: 1)')
    parser.add_argument('--num-gpus', '-g', type=str, default='1', help='Number of gpus per pod (default: 1)')
    parser.add_argument('--delay', type=float, default=0.0, help='Number of seconds to sleep between submission iterations (default: 0.0)')
    parser.add_argument('--project', type=str, default=DEFAULT_PROJECT, help='Project to submit to')
    parser.add_argument('--pvc', action='store_true', default=False, help='Add PVC to the submitted workloads (default: False)')

    args = parser.parse_args()

    if not os.path.isdir(args.output_dir):
        os.mkdir(args.output_dir)

    times = submit_workloads(args.output_dir, args.workload_type, args.num_workloads, args.num_processes, args.delay,
                             args.project, args.num_gpus, args.num_workers, args.pvc)
    logging.info("writing final json")
    write_json(args.output_dir, times)
