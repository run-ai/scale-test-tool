#!/usr/bin/env python

import os
import re
import argparse
from collections import defaultdict
from datetime import datetime
import json
import matplotlib.pyplot as plt

from settings import *

REFERENCE_EVENT = {
    'Workload Creation': 'Workload Submission',
    'Job Creation': 'Workload Creation',
    'First Pod Creation': 'Job Creation',
    'Last Pod Creation': 'Job Creation',
    'Pod Group Creation': 'First Pod Creation',
    'Pod Scheduling Decision': 'Pod Group Creation',
    'First Eviction': 'Pod Group Creation',
    'First PVC Bind Request': 'Pod Group Creation',
    'First PVC Bind': 'First PVC Bind Request',
    'Backend Job Creation': 'Pod Group Creation',

    'Total Pod Scheduling Decision': 'Workload Submission',
    'Total Backend Job Creation': 'Workload Submission'
}


def parse_timestamp_from_log(job_info, timestamp_str):
    # handling different time string formats.
    # removing sub-second info if such, as in timestamps we get from the cluster we only have seconds, and we don't
    # want to have negative times (e.g. creation < submission as 10:45:00 < 10:45:00.542331)
    t = job_info[timestamp_str]

    t = t.replace('T', ' ')

    for c in ['.', 'Z', '+']:
        if c in t:
            c_index = t.index(c)
            t = t[:c_index]

    dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    return dt


def calculate_time_diff_from_log(event_timestamp, reference_timestamp):
    diff = (event_timestamp - reference_timestamp).total_seconds()
    # ignore negative values that are close to 0, as those are a result of inaccuracies and different resolutions in measurements
    if diff < 0 and diff >= -1:
        diff = 0
    return diff


def get_job_info_items_from_jsons(submitted_json_path, sampled_json_path):
    job_info_list = []

    submitted = json.load(open(submitted_json_path, 'r'))
    sampled = json.load(open(sampled_json_path, 'r'))

    # we need to take submitTimestamp from submitted, and add it to the relevant item in sampled
    # to do that, we first build a dictionary from (jobName, projectName) to submitTimestamp
    submit_time_dict = {}
    for job_info in submitted:
        k = (job_info["jobName"], job_info["projectName"])
        v = job_info["submitTimestamp"]
        submit_time_dict[k] = v

    for job_info in sampled:
        k = (job_info["jobName"], job_info["projectName"])
        try:
            v = submit_time_dict[k]
        except KeyError:
            print(f"missing submit time for job {job_info['jobName']} project {job_info['projectName']}, skipping")
            continue
        job_info["submitTimestamp"] = v

        job_info_list.append(job_info)

    return job_info_list


def get_png_file_path(output_dir, plot_type):
    png_file_path = os.path.join(output_dir, f"{plot_type}.png")

    return png_file_path


def get_csv_file_path(output_dir):
    csv_file_path = os.path.join(output_dir, "times.csv")

    return csv_file_path


def open_csv(output_dir):
    csv_file_path = get_csv_file_path(output_dir)
    csv_file = open(csv_file_path, 'wt')
    csv_events = list(REFERENCE_EVENT.keys())

    job_info_fields = ['Job Name', 'Namespace', 'projectName', 'Workload Created', 'Job Created', 'First Pod Created', 'Last Pod Created', 'Pod Group Created', 'Pod Scheduling Decision', 'Workload Submitted', 'Backend Job Created']

    csv_fields = job_info_fields + csv_events
    csv_file.write(','.join(csv_fields) + '\n')
    return csv_file, csv_events


def write_csv_data_line(csv_file, csv_events, job_info, data):
    csv_values = [str(e) for e in job_info.values()] + [str(data[e][-1]) for e in csv_events]
    csv_file.write(','.join(csv_values) + '\n')


def parse_data(output_dir, skip_errors, head, tail):
    job_info_list = get_job_info_items_from_jsons(f'{output_dir}/submitted.json', f'{output_dir}/sampled.json')

    csv_file, csv_events = open_csv(output_dir)

    data = defaultdict(list)
    num_errors = 0

    for job_info in job_info_list:
        # extract timestamps
        for event in job_info.keys():
            if 'Time' not in event:
                continue
            job_info[event] = parse_timestamp_from_log(job_info, event)

        # relative time - each event time is calculated as delta from the previous one
        workload_submission = job_info['submitTimestamp']
        workload_creation = calculate_time_diff_from_log(job_info['workloadCreatedTimestamp'], job_info['submitTimestamp'])
        job_creation = calculate_time_diff_from_log(job_info['jobCreatedTimestamp'], job_info['workloadCreatedTimestamp'])
        first_pod_creation = calculate_time_diff_from_log(job_info['firstPodCreatedTimestamp'], job_info['jobCreatedTimestamp'])
        last_pod_creation = calculate_time_diff_from_log(job_info['lastPodCreatedTimestamp'], job_info['jobCreatedTimestamp'])
        pod_group_creation = calculate_time_diff_from_log(job_info['podGroupCreatedTimestamp'], job_info['firstPodCreatedTimestamp'])
        first_eviction = calculate_time_diff_from_log(job_info['firstEvictionTimestamp'], job_info['podGroupCreatedTimestamp'])
        first_pvc_bind_request = calculate_time_diff_from_log(job_info['firstPVCBindRequestTimestamp'], job_info['podGroupCreatedTimestamp'])
        first_pvc_bind = calculate_time_diff_from_log(job_info['firstPVCBindTimestamp'], job_info['firstPVCBindTimestamp'])
        pod_scheduling_decision = calculate_time_diff_from_log(job_info['podSchedulingDecisionTimestamp'], job_info['podGroupCreatedTimestamp'])
        backend_job_creation = calculate_time_diff_from_log(job_info['backendJobCreatedTimestamp'], job_info['podGroupCreatedTimestamp'])

        # total time - each event time is calculated as delta from submission time
        total_pod_scheduling_decision = calculate_time_diff_from_log(job_info['podSchedulingDecisionTimestamp'], job_info['submitTimestamp'])
        total_backend_job_creation = calculate_time_diff_from_log(job_info['backendJobCreatedTimestamp'], job_info['submitTimestamp'])

        # handle errors
        if skip_errors:
            # if pod group time is earlier than the first pod time, it means
            # the pod was killed and replaced with a new one. can happend for example
            # in aws spot termination case. in this case we cannot rely on the time deltas
            # we calculated, so we skip
            if job_info['podGroupCreatedTimestamp'] < job_info['firstPodCreatedTimestamp']:
                print(f"podgroup time earlier than pod time for job {job_info['jobName']} project {job_info['projectName']}, skipping")
                num_errors += 1
                continue

            if total_pod_scheduling_decision < 0:
                print(f"total time to scheduling decision is negative for job {job_info['jobName']} project {job_info['projectName']}, skipping")
                num_errors += 1
                continue

        data['Workload Submission'].append(workload_submission)
        data['Workload Creation'].append(workload_creation)
        data['Job Creation'].append(job_creation)
        data['First Pod Creation'].append(first_pod_creation)
        data['Last Pod Creation'].append(last_pod_creation)
        data['Pod Group Creation'].append(pod_group_creation)
        data['First Eviction'].append(first_eviction)
        data['First PVC Bind Request'].append(first_pvc_bind_request)
        data['First PVC Bind'].append(first_pvc_bind)
        data['Pod Scheduling Decision'].append(pod_scheduling_decision)
        data['Backend Job Creation'].append(backend_job_creation)

        data['Total Pod Scheduling Decision'].append(total_pod_scheduling_decision)
        data['Total Backend Job Creation'].append(total_backend_job_creation)

        if csv_file:
            write_csv_data_line(csv_file, csv_events, job_info, data)

    if skip_errors and num_errors > 0:
        print(f"total of {num_errors} (out of {len(job_info_list)}) jobs with errors were found and skipped")

    # sort all the lists together, by submit_timestamp.
    # needed since the tests may be run in parallel and log order is not guaranteed
    sorted_data = {key: [value for _, value in sorted(zip(data['Workload Submission'], data[key]))] for key in data}

    # handle head and tail
    if head or tail:
        for list_name in sorted_data.keys():
            if head:
                sorted_data[list_name] = sorted_data[list_name][:head]
            if tail:
                sorted_data[list_name] = sorted_data[list_name][-tail:]

    if csv_file:
        csv_file.close()

    return sorted_data


def create_graphs_general(data, png_file_path):
    events = ['Total Pod Scheduling Decision']
    if TEST_BACKEND_TIMES:
        events += ['Total Backend Job Creation']

    fig, axs = plt.subplots(len(events), 1, figsize=(15, 3*len(events)))
    title = f"Total number of jobs run: {len(data[events[0]])}"
    fig.suptitle(title, fontsize=20)

    for i, event in enumerate(events):
        event_times = data[event]
        average_time = sum(event_times)/len(event_times)
        if len(events) > 1:
            ax = axs[i]
        else:
            ax = axs
        ax.plot(event_times)
        ax.set_xlabel('Number of jobs')
        ax.set_ylabel('Time (seconds)')
        ax.set_title(f'{event} Time from {REFERENCE_EVENT[event]} (average: {average_time:.2f} seconds)')

    plt.tight_layout()

    plt.savefig(png_file_path)

    plt.show()

    return plt


def create_graphs_detailed(data, png_file_path):
    events = ['Workload Creation', 'Job Creation', 'First Pod Creation', 'Last Pod Creation', 'Pod Group Creation', 'Pod Scheduling Decision']
    if TEST_SCHEDULER_EVENT_TIMES:
        events += ['First Eviction', 'First PVC Bind Request', 'First PVC Bind']
    if TEST_BACKEND_TIMES:
        events += ['Backend Job Creation']

    fig, axs = plt.subplots(len(events), 2, figsize=(30, 1.7*len(events)))
    title = f"Total number of jobs run: {len(data[events[0]])}"
    fig.suptitle(title, fontsize=20)

    for i, event in enumerate(events):
        event_times = data[event]
        average_time = sum(event_times)/len(event_times)
        ax = axs[i, 0]
        ax.plot(data['Workload Submission'], event_times)
        ax.set_xlabel('Submit Timestamp')
        ax.set_ylabel('Time (seconds)')
        ax.set_title(f'Time between {REFERENCE_EVENT[event]} and {event} (average: {average_time:.2f} seconds)')

        ax = axs[i, 1]
        _, bins, _ = ax.hist(event_times)
        ax.set_xlabel('Time (seconds)')
        ax.set_ylabel('Number of jobs')
        # ax.set_xticks(bins)
        ax.set_title(f'Time between {REFERENCE_EVENT[event]} and {event} (average: {average_time:.2f} seconds)')

    plt.tight_layout()

    plt.savefig(png_file_path)

    plt.show()

    return plt


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', '-o', type=str, default=DEFAULT_OUTPUT_DIR, help='Output dir')
    parser.add_argument('--plot', choices=['general', 'detailed'], default='general',
                        help='Plot type (default: general)')
    parser.add_argument('--skip-errors', action='store_true', default=True,
                        help='Skip workloads with erroneous times, excluding them from the graphs (default: True)')
    parser.add_argument('--head', type=int, default=None, help='Process only this number of workloads from the start')
    parser.add_argument('--tail', type=int, default=None, help='Process only this number of workloads from the end')

    args = parser.parse_args()

    if not os.path.isdir(args.output_dir):
        print(f"output dir '{args.output_dir}' doesn't exist")
        exit(1)

    data = parse_data(args.output_dir, args.skip_errors, args.head, args.tail)

    if len(data) == 0:
        print("no data")
        exit()

    png_file_path = get_png_file_path(args.output_dir, args.plot)

    if args.plot == 'general':
        plt = create_graphs_general(data, png_file_path)
    else:
        plt = create_graphs_detailed(data, png_file_path)

