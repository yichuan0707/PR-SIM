import csv
import re
import os
from time import time, mktime, strptime
from simulator.Configuration import BASE_PATH

LANL_TRACE_DIR = BASE_PATH + "traces" + os.sep + "LANL" + os.sep
PNNL_TRACE_PATH = BASE_PATH + "traces" + os.sep + "PNNL2007.csv"
CSV_PATH = BASE_PATH + "traces" + os.sep + "csv" + os.sep

PNNL_START_TIME = mktime(strptime('11/29/2003 7:00', '%m/%d/%Y %H:%M'))


# Return every node's TTF(node's recent failed - node's last failed)
def get_pnnl_failures(disk_ttf_flag=True):
    """
    disk ttf flag: True means the statistics for disks;
                   False means the statistics for system.
    """
    failed_components = []
    disk_failures = {}
    count = 0

    with open(PNNL_TRACE_PATH, 'r') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if row['What Failed'] not in failed_components:
                failed_components.append(row['What Failed'])

            if row['Action Taken'] == 'REPLACE' and \
               row['What Failed'] == 'DISK':
                count += 1
                msg = row['Description of Failure']

                # Here, we should use 're' module for matching 'sd.'.
                failed_disks = re.findall('[s|S][d|D].', msg)
                count += len(failed_disks) - 1
                for item in failed_disks:
                    node_disk = row['Hardware Identifier'].lower() + \
                                ':' + item.lower()
                    failed_time = mktime(strptime(row['Date'],
                                                  '%m/%d/%Y %H:%M'))
                    try:
                        previous_failed_time = disk_failures.pop(node_disk)
                        previous_failed_time.append(failed_time)
                        disk_failures[node_disk] = previous_failed_time
                    except KeyError:
                        disk_failures[node_disk] = [failed_time]

    failures = {}
    failed_disks = disk_failures.keys()

    for disk in failed_disks:
        failed_timestamps = disk_failures[disk]
        failed_timestamps.sort()
        fail_time = len(failed_timestamps)
        if disk_ttf_flag:
            failed_timestamps.insert(0, PNNL_START_TIME)
            # translate seconds into days.
            failures[disk] = [round((failed_timestamps[i+1] -
                                    failed_timestamps[i])/(3600*24), 2)
                              for i in xrange(fail_time)]
        else:  # system level's ttf, for all nodes
            failures[disk] = [round((failed_timestamps[i] - PNNL_START_TIME) /
                                    (3600*24), 8) for i in xrange(fail_time)]

    failure_count = [0 for i in xrange(10)]
    key_list = failures.keys()
    for key in key_list:
        length = len(failures[key])
        failure_count[length-1] += 1
        if length == 1:
            failures.pop(key)

    print failure_count
    print failures

    return failures


# sys_id between 3-20
def get_lanl_failures(sys_id, node_ttf_flag=True):
    """
    Node ttf flag: True means the statistics for each node;
                   False means the statistics for whole system.
    """
    trace_path = LANL_TRACE_DIR + str(sys_id)
    transient_ttf_failures = {}
    transient_ttr_failures = {}
    permenant_ttf_failures = {}
    permenant_ttr_failures = {}

    with open(trace_path, 'r') as fp:
        trace_start_time = time()
        for line in fp:
            res = line.split(',')
            print float(res[3])
            trace_start_time = min(float(res[3]), trace_start_time)
        print "\n\n", trace_start_time
        fp.seek(0, 0)
        for i, line in enumerate(fp):
            res = line.split(',')
            node_id = res[0] + '-' + res[2]
            if res[-1] == 'transient\n':
                try:
                    previous_failed_time = transient_ttf_failures.pop(node_id)
                    new_failure = (float(res[3]) - trace_start_time)/(3600*24)
                    if node_ttf_flag:
                        new_failure -= sum(previous_failed_time)
                    previous_failed_time.append(round(new_failure, 8))
                    transient_ttf_failures[node_id] = previous_failed_time
                except KeyError:
                    transient_ttf_failures[node_id] = [round(((float(res[3]) -
                                                       trace_start_time) /
                                                       (3600*24)), 8)]

                try:
                    previous_repair_time = transient_ttr_failures.pop(node_id)
                    # TTR in mins
                    previous_repair_time.append(float(res[5]))
                    transient_ttr_failures[node_id] = previous_repair_time
                except KeyError:
                    transient_ttr_failures[node_id] = [float(res[5])]
            elif res[-1] == 'permanent\n':
                try:
                    previous_failed_time = permenant_ttf_failures.pop(node_id)
                    # TTF in days
                    new_failure = (float(res[3]) - trace_start_time)/(3600*24)
                    if node_ttf_flag:
                        new_failure -= sum(previous_failed_time)
                    previous_failed_time.append(round(new_failure, 8))
                    permenant_ttf_failures[node_id] = previous_failed_time
                except KeyError:
                    permenant_ttf_failures[node_id] = [round((float(res[3]) -
                                                       trace_start_time) /
                                                       (3600*24), 8)]

                try:
                    previous_repair_time = permenant_ttr_failures.pop(node_id)
                    # TTR in mins
                    previous_repair_time.append(float(res[5]))
                    permenant_ttr_failures[node_id] = previous_repair_time
                except KeyError:
                    permenant_ttr_failures[node_id] = [float(res[5])]
            else:
                raise Exception("Wrong Error Type!")

    return (transient_ttf_failures, transient_ttr_failures,
            permenant_ttf_failures, permenant_ttr_failures)


def put_into_csv(failures, trace_name):
    ttfs = []
    key_list = failures.keys()

    for key in key_list:
        ttfs += failures[key]
    ttfs.sort()

    path = CSV_PATH + trace_name
    with open(path, 'w') as fp:
        writer = csv.writer(fp, lineterminator='\n')
        for item in ttfs:
            writer.writerow([item])


if __name__ == "__main__":
    # put_into_csv(get_pnnl_failures(False), "pnnl_ttf_sys_lifetime.csv")
    """
    for i in [3, 4, 5, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20]:
        name_list = ["lanl_" + str(i) + "_tran_ttf_sys.csv",
                     "lanl_" + str(i) + "_tran_ttr_sys.csv",
                     "lanl_" + str(i) + "_per_ttf_sys.csv",
                     "lanl_" + str(i) + "_per_ttr_sys.csv"]
        t_ttf, t_ttr, p_ttf, p_ttr = get_lanl_failures(i, False)
        put_into_csv(t_ttf, name_list[0])
        put_into_csv(t_ttr, name_list[1])
        put_into_csv(p_ttf, name_list[2])
        put_into_csv(p_ttr, name_list[3])
    """
"""
    name_list = ["lanl_all_tran_ttf_sys.csv",
                 "lanl_all_tran_ttr_sys.csv",
                 "lanl_all_per_ttf_sys.csv",
                 "lanl_all_per_ttr_sys.csv"]
    t_ttf_all = {}
    t_ttr_all = {}
    p_ttf_all = {}
    p_ttr_all = {}
    for i in [3, 4, 5, 8, 9, 10, 11, 12, 13, 14, 18, 19, 20]:
        t_ttf, t_ttr, p_ttf, p_ttr = get_lanl_failures(i)
        t_ttf_keys = t_ttf.keys()
        t_ttr_keys = t_ttr.keys()
        p_ttf_keys = p_ttf.keys()
        p_ttr_keys = p_ttr.keys()
        for key in t_ttf_keys:
            t_ttf_all[key] = t_ttf[key]
        for key in t_ttr_keys:
            t_ttr_all[key] = t_ttr[key]
        for key in p_ttf_keys:
            p_ttf_all[key] = p_ttf[key]
        for key in p_ttr_keys:
            p_ttr_all[key] = p_ttr[key]

    put_into_csv(t_ttf_all, name_list[0])
    put_into_csv(t_ttr_all, name_list[1])
    put_into_csv(p_ttf_all, name_list[2])
    put_into_csv(p_ttr_all, name_list[3])
"""
