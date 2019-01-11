import csv
import re
import os
from time import mktime, strptime, localtime, strftime

BASE_PATH = os.path.dirname(os.path.dirname(os.getcwd()))
TRACE_DIR = BASE_PATH + os.sep + "traces" + os.sep
LANL_TRACE_PATH = TRACE_DIR + "lanl_events.tab"
PNNL_TRACE_PATH = TRACE_DIR + "PNNL2007.csv"
CSV_DIR = TRACE_DIR + "csv" + os.sep


class PNNLProcess(object):
    trace_path = PNNL_TRACE_PATH

    def __init__(self):
        self.start_time = self._time_to_second('11/29/2003 7:00')

        # Only one schedule outage reported in PNNL, and it caused more than
        # 40 failures during 3 days, 38 of them were disk replacements.
        self.reported_correlated = (self._time_to_second('3/22/2005 14:30'),
                                    self._time_to_second('3/25/2005 16:00'))
        self.moved_report_correlated = False

        # Move system disk failures from disk undurable to node
        # unavailable(True) or not(False)
        self.move_system_disk_failures = False

        self.traces = []
        self.actions = {}
        self.failed_components = {}

        with open(PNNLProcess.trace_path) as fp:
            reader = csv.reader(fp)
            for item in reader:
                if item[0] == "Date":
                    continue
                self.traces.append(item)

                self.failed_components[item[2]] = 1 + \
                    self.failed_components.pop(item[2], 0)
                self.actions[item[-1]] = 1 + self.actions.pop(item[-1], 0)

        (self.disk_failures, self.node_lost, self.node_unavailable,
         self.power_outage, self.crashes, self.ignore_events) = \
            self._classfiy_events()

        # self.disk_failures = self._get_disk_failures()

    # This function implements as the blindly collecting disk failures.
    def _get_disk_failures(self):
        disk_failures = {}
        unknown_disk_failure = 0
        known_disk_failure = 0
        others = 0
        for failure in self.traces:
            msg = failure[-2]
            failed_node = failure[1].lower()

            # Here, we should use 're' module for matching 'sd.'.
            failed_disks = re.findall('[s|S][d|D].', msg)

            # Most Disk Replacements marked as "DISK" with "REPLACE", but a
            # few of them with "REBOOT".
            if failure[2] == "DISK" and failure[-1] in ["REPLACE", "REBOOT"]:
                # Part of Replacements are not given the name of the Disk
                # Drive, use "unknown" represents the failed drive
                if failed_disks == [] and failure[-1] == "REPLACE":
                    node_disk = failed_node.lower() + ":unknown"
                    failed_time = mktime(strptime(failure[0],
                                                  '%m/%d/%Y %H:%M'))
                    disk_failures[node_disk] = disk_failures.pop(node_disk,
                                                                 []) \
                        + [failed_time]
                    unknown_disk_failure += 1
                for item in failed_disks:
                    node_disk = failed_node.lower() + ":" + item.lower()
                    failed_time = mktime(strptime(failure[0],
                                                  '%m/%d/%Y %H:%M'))
                    disk_failures[node_disk] = disk_failures.pop(node_disk,
                                                                 []) \
                        + [failed_time]
                    known_disk_failure += 1

            # A little part of disk replacements marked with CNTLR(controller),
            # SCSI_BP(blackplane of SCSI), or PLATFORM.
            # "CABLE" failure might cause disk replacement, either. But we are
            # not sure about the possibility.
            if failed_disks != [] and failure[-1] == "REPLACE" and \
               failure[2] in ["CNTLR", "SCSI_BP", "PLATFORM"]:
                for item in failed_disks:
                    node_disk = failed_node.lower() + ":" + item.lower()
                    failed_time = self._time_to_second(failure[0])
                    disk_failures[node_disk] = disk_failures.pop(node_disk,
                                                                 []) \
                        + [failed_time]
                others += len(failed_disks)

        return disk_failures

    def _time_to_second(self, format_time):
        return mktime((strptime(format_time, '%m/%d/%Y %H:%M')))

    def _second_to_time(self, seconds):
        return strftime('%m/%d/%Y %H:%M', localtime(seconds))

    # (1)self.node_unavailable_events include node lost, power outage and
    #     crash.
    #     We can use the "self.move_system_disk_failures" controll whether we
    #     move system disk failures to self.node_unavilable_events or not.
    # (2) Ignore all the switch failures, because we don't konw the network
    #    architecture, and some switch failures have been recorded in the node
    #    failures.
    # (3) Ignore all "HSV" failures, maybe it's one kind of disk replacement,
    #     but we do not know it clearly, and the nodes with "HSV" failures are
    #     strange, such dtemp, home or test. Maybe they ran special Lustre
    #     services.
    def _classfiy_events(self):
        # key for disk failures is "node:disk",
        # key for others is "node"
        disk_failures = {}
        node_lost = {}
        power_outage = {}
        node_unavailable_events = {}
        crashes = {}
        ignore_events = {}

        self.disk_undurable_events = {}
        self.node_undurable_events = {}
        self.node_unavailable_events = {}

        unknown_disk_failure = 0
        known_disk_failure = 0

        for item in self.traces:
            msg = item[-2]
            failed_node = item[1].lower()

            # Skip the switch error, we don't know the network architecture.
            if item[1].find("switch") != -1 or item[1].find("Switch") != -1:
                continue

            # Here, we should use 're' module for matching 'sd.'.
            failed_disks = re.findall('[s|S][d|D].', msg)

            failed_time = self._time_to_second(item[0])

            # Most Disk Replacements marked as "DISK" with "REPLACE", but a
            # few of them with "REBOOT".
            if item[-1] in ["REPLACE", "REBOOT"] and item[2] == "DISK":
                # Part of Replacements are not given the name of the Disk
                # Drive, use "unknown" represents the failed drive
                if failed_disks == [] and item[-1] == "REPLACE":
                    node_disk = failed_node.lower() + ":unknown"
                    disk_failures[node_disk] = disk_failures.pop(node_disk,
                                                                 []) \
                        + [failed_time]
                    self.disk_undurable_events[failed_time] = \
                        self.disk_undurable_events.pop(failed_time, []) + \
                        [node_disk]
                    unknown_disk_failure += 1
                for disk in failed_disks:
                    node_disk = failed_node.lower() + ":" + disk.lower()
                    disk_failures[node_disk] = disk_failures.pop(node_disk,
                                                                 []) \
                        + [failed_time]
                    self.disk_undurable_events[failed_time] = \
                        self.disk_undurable_events.pop(failed_time, []) + \
                        [node_disk]
                    known_disk_failure += 1
            elif item[-1] in ["REPLACE", "REBOOT"] and item[2] != "DISK":
                if item[2] == "HSV":
                    # HSV is about Luste-fs, dtemp storage and test node.
                    # Don't know how to process yet.
                    pass
                elif failed_disks != []:
                    for disk in failed_disks:
                        node_disk = failed_node.lower() + ":" + disk.lower()
                        disk_failures[node_disk] = disk_failures.pop(
                            node_disk, []) + [failed_time]
                        self.disk_undurable_events[failed_time] = \
                            self.disk_undurable_events.pop(failed_time, []) + \
                            [node_disk]
                        known_disk_failure += 1
                    node_unavailable_events[item[1]] = \
                        node_unavailable_events.pop(item[1], []) + \
                        [failed_time]
                    self.node_unavailable_events[failed_time] = \
                        self.node_unavailable_events.pop(failed_time, []) + \
                        [failed_node]
                elif item[2] == "PLATFORM":
                    if re.findall("[r|R]eplaced system board", item[-2]) == \
                       [] and re.findall("returned to cluster", item[-2]) == \
                       [] and re.findall("board replaced", item[-2]) == []:
                        node_lost[item[1]] = node_lost.pop(item[1], []) + \
                            [failed_time]
                        self.node_undurable_events[failed_time] = \
                            self.node_undurable_events.pop(failed_time, []) + \
                            [failed_node]

                    # node lost also cause node unavailable.
                    node_unavailable_events[item[1]] = \
                        node_unavailable_events.pop(item[1], []) + \
                        [failed_time]
                    self.node_unavailable_events[failed_time] = \
                        self.node_unavailable_events.pop(failed_time, []) + \
                        [failed_node]
                else:
                    # Power outage and crash may cause data loss, so we need
                    # to collect them alone, meanwhile, all of them will cause
                    # node unavailable.
                    if item[2] == "PS":
                        power_outage[item[1]] = power_outage.pop(item[1], []) \
                            + [failed_time]
                    elif msg.find("crash") != -1 or msg.find("Crash") != -1:
                        crashes[item[1]] = crashes.pop(item[1], []) + \
                            [failed_time]
                    else:
                        pass
                    node_unavailable_events[item[1]] = \
                        node_unavailable_events.pop(item[1], []) + \
                        [failed_time]
                    self.node_unavailable_events[failed_time] = \
                        self.node_unavailable_events.pop(failed_time, []) + \
                        [failed_node]
            else:
                if msg.find("crash") != -1 or msg.find("Crash") != -1:
                    crashes[item[1]] = crashes.pop(item[1], []) + \
                        [failed_time]
                    node_unavailable_events[item[1]] = \
                        node_unavailable_events.pop(item[1], []) + \
                        [failed_time]
                    self.node_unavailable_events[failed_time] = \
                        self.node_unavailable_events.pop(failed_time, []) + \
                        [failed_node]

                # Some failures have IO error on hard drive(maybe caused disk
                # Replacement), or reboot node(node unavailable), we treat
                # all of them as ignore event.
                if item[-1] in ["NONE", "INVESTIGATE", "PENDING"]:
                    ignore_events[failed_node] = \
                        ignore_events.pop(failed_node, []) + [failed_time]
                elif item[-1] in ["INSTALL", "Outage"]:
                    for disk in failed_disks:
                        node_disk = failed_node.lower() + ":" + disk.lower()
                        disk_failures[node_disk] = disk_failures.pop(
                            node_disk, []) + [failed_time]
                        self.disk_undurable_events[failed_time] = \
                            self.disk_undurable_events.pop(failed_time, []) + \
                            [node_disk]
                        known_disk_failure += 1
                    if len(re.findall("I/O Error", msg)) != len(failed_disks):
                        node_disk = failed_node.lower() + ":" + "unknown"
                        disk_failures[node_disk] = disk_failures.pop(
                            node_disk, []) + [failed_time]
                        self.disk_undurable_events[failed_time] = \
                            self.disk_undurable_events.pop(failed_time, []) + \
                            [node_disk]
                        unknown_disk_failure += 1
                    else:
                        node_unavailable_events[item[1]] = \
                            node_unavailable_events.pop(item[1], []) + \
                            [failed_time]
                        self.node_unavailable_events[failed_time] = \
                            self.node_unavailable_events.pop(failed_time, []) \
                            + [failed_node]
                else:
                    node_unavailable_events[item[1]] = \
                        node_unavailable_events.pop(item[1], []) + \
                        [failed_time]
                    self.node_unavailable_events[failed_time] = \
                        self.node_unavailable_events.pop(failed_time, []) + \
                        [failed_node]

        # Move system disk failures from disk undurable to node unavailable.
        # For 0<= node_id <= 569, system disk is sda and sdd, but for
        # 570<= node_id <= 979, system disk is sda and sdb, so, we remove the
        # system disk from undurable to unavailable based on the drive name.
        if self.move_system_disk_failures:
            failed_disks = disk_failures.keys()
            for disk in failed_disks:
                node_name, disk_name = disk.split(":")
                node_id = int(re.findall("[0-9]+", node_name)[0])
                if (node_id < 570 and disk_name in ["sda", "sdd"]) or \
                   (node_id >= 570 and disk_name in ["sda", "sdb"]):
                    ts_list = disk_failures.pop(disk)
                    node_unavailable_events[node_name] = \
                        node_unavailable_events.pop(node_name, []) + \
                        ts_list
                    for ts in ts_list:
                        # print self.disk_undurable_events
                        disk_list = self.disk_undurable_events[ts]
                        disk_list.remove(disk)
                        self.node_unavailable_events[ts] = \
                            self.node_unavailable_events.pop(ts, []) + \
                            [disk]

        # Remove the report correlated disk failures.
        if self.moved_report_correlated:
            failed_disks = disk_failures.keys()
            for disk in failed_disks:
                ts = disk_failures[disk]
                for item in ts:
                    if item >= self.reported_correlated[0] and \
                       item <= self.reported_correlated[1]:
                        ts.remove(item)
                    if ts == []:
                        disk_failures.pop(disk)

            keys = self.disk_undurable_events.keys()
            for key in keys:
                if key >= self.reported_correlated[0] and \
                   key <= self.reported_correlated[1]:
                    self.disk_undurable_events.pop(key)

        count = 0
        node_lost_count = 0
        node_unavailable_count = 0
        power_outage_count = 0
        ignore_count = 0
        for key in disk_failures.keys():
            count += len(disk_failures[key])
        for key in node_lost.keys():
            node_lost_count += len(node_lost[key])
        for key in node_unavailable_events.keys():
            node_unavailable_count += len(node_unavailable_events[key])
        for key in power_outage.keys():
            power_outage_count += len(power_outage[key])
        for key in crashes.keys():
            power_outage_count += len(crashes[key])
        for key in ignore_events.keys():
            ignore_count += len(ignore_events[key])

        print "disk failures count: ", len(disk_failures.keys()), count
        print "node lost count: ", len(node_lost.keys()), node_lost_count
        print "node unavailable count: ", len(node_unavailable_events.keys()),\
            node_unavailable_count
        print "node outage count:", len(power_outage.keys()) + \
            len(crashes.keys()), power_outage_count
        print "ignore events count:", len(ignore_events.keys()), ignore_count

        return (disk_failures, node_lost, node_unavailable_events,
                power_outage, crashes, ignore_events)

    def sys_disk_undurable_ttfs(self):
        tmp = []
        sys_disk_undur = []
        disks = self.disk_failures.keys()
        # print disks
        for disk in disks:
            tmp += self.disk_failures[disk]
        tmp.sort()

        len_undur = len(tmp)
        for i in xrange(len_undur):
            pre_fail_time = self.start_time if i == 0 else tmp[i-1]
            ttf = float(tmp[i] - pre_fail_time)/3600
            sys_disk_undur.append(round(ttf, 3))

        sys_disk_undur.sort()
        return sys_disk_undur

    # Remove and ignore the failures of Unknown.
    def disk_ttfs(self, lifetime=False, exclude_correlated=False):
        disks_failure_timestamps = []
        if exclude_correlated:
            disks = self.disk_failures.keys()
            correlated_in_disk_undurable = \
                self._find_correlates(self.disk_undurable_events)
            correlated_failed = {}
            for one_correlate in correlated_in_disk_undurable:
                for item in one_correlate:
                    correlated_failed[item[1]] = \
                        correlated_failed.pop(item[1], []) + \
                        [item[0]]
            correlated_failed_disks = correlated_failed.keys()

            for disk in disks:
                if disk in correlated_failed_disks:
                    for ts in correlated_failed[disk]:
                        tmp = self.disk_failures[disk]
                        tmp.remove(ts)
                        if self.disk_failures[disk] == []:
                            self.disk_failures.pop(disk)
                        else:
                            self.disk_failures[disk] = tmp

        disks = self.disk_failures.keys()

        one_count = 0
        two_count = 0
        three_count = 0
        four_count = 0
        five_count = 0
        six_count = 0

        for disk in disks:
            if lifetime and disk.endswith("unknown"):
                continue
            timestamps = self.disk_failures[disk]
            timestamps.sort()
            if len(timestamps) == 1:
                one_count += 1
            elif len(timestamps) == 2:
                two_count += 1
            elif len(timestamps) == 3:
                three_count += 1
            elif len(timestamps) == 4:
                four_count += 1
            elif len(timestamps) == 5:
                five_count += 1
            elif len(timestamps) == 6:
                six_count += 1
            else:
                pass

            for i in xrange(len(timestamps)):
                if i == 0:
                    if lifetime:
                        continue
                    else:
                        time_in_hours = float(timestamps[i] -
                                              self.start_time)/3600
                else:
                    time_in_hours = float(timestamps[i] -
                                          timestamps[i-1])/3600
                disks_failure_timestamps.append(round(time_in_hours, 3))
        disks_failure_timestamps.sort()

        print "disk ttfs: ", len(disks), len(disks_failure_timestamps)
        print "one failure: ", one_count
        print "two failures: ", two_count
        print "three failures: ", three_count
        print "four failures: ", four_count
        print "five failures: ", five_count
        print "six failures: ", six_count

        return disks_failure_timestamps

    # nodes(>=2) failed at same timestamp
    # format: {ts:[node1, node2, ...]}
    def simultaneous_node_lost(self):
        simultaneous_node_failures = {}
        keys = self.node_lost.keys()
        for key in keys:
            for item in self.node_lost[key]:
                simultaneous_node_failures[item] = \
                    simultaneous_node_failures.pop(item, []) + [key]

        sim_keys = simultaneous_node_failures.keys()
        for key in sim_keys:
            if len(simultaneous_node_failures[key]) == 1:
                simultaneous_node_failures.pop(key)

        return simultaneous_node_failures

    # From system.
    def sys_node_undurable_ttfs(self):
        tmp_ts = []
        node_undurable = []

        for key in self.node_lost.keys():
            tmp_ts += self.node_lost[key]
        tmp_ts.sort()
        len_node_undur = len(tmp_ts)
        for i in xrange(len_node_undur):
            if i == 0:
                # ttf in hours
                ttf = float(tmp_ts[i] - self.start_time)/3600
            else:
                ttf = float(tmp_ts[i] - tmp_ts[i-1])/3600
            node_undurable.append(round(ttf, 3))

        node_undurable.sort()
        return node_undurable

    def node_undurable_ttfs(self, lifetime=False, exclude_correlated=False):
        if exclude_correlated:
            nodes = self.node_lost.keys()
            correlated_in_node_undurable = \
                self._find_correlates(self.node_undurable_events)
            correlated_failed = {}
            for one_correlate in correlated_in_node_undurable:
                for item in one_correlate:
                    correlated_failed[item[1]] = \
                        correlated_failed.pop(item[1], []) + \
                        [item[0]]
            correlated_failed_nodes = correlated_failed.keys()

            for node in nodes:
                if node in correlated_failed_nodes:
                    for ts in correlated_failed[node]:
                        tmp = self.node_lost[node]
                        tmp.remove(ts)
                        if self.node_lost[node] == []:
                            self.node_lost.pop(node)
                        else:
                            self.node_lost[node] = tmp

        node_undurable = []
        keys = self.node_lost.keys()
        for key in keys:
            ts = self.node_lost[key]
            ts.sort()
            for i in xrange(len(ts)):
                if i == 0:
                    if lifetime:
                        continue
                    else:
                        # ttf in hours
                        ttf = float(ts[i] - self.start_time)/3600
                else:
                    ttf = float(ts[i] - ts[i-1])/3600
                node_undurable.append(round(ttf, 3))
        node_undurable.sort()
        print "node undurable: ", len(keys), len(node_undurable)

        return node_undurable

    def _find_correlates(self, events_dict, time_threshold=120):
        correlates = []

        timestamps = events_dict.keys()
        timestamps.sort()

        one_correlate = []
        last_ts = timestamps[0]
        for ts in timestamps:
            tmp = events_dict.pop(ts)
            if ts - last_ts <= time_threshold:
                for item in tmp:
                    one_correlate.append((ts, item))
            else:
                if len(one_correlate) > 1:
                    correlates.append(one_correlate)
                    one_correlate = []
                else:
                    one_correlate = []
                    for item in tmp:
                        one_correlate.append((ts, item))
                last_ts = ts

        return correlates

    def _correlates_process(self, data_source):
        correlates = {}
        for item in data_source:
            time_interval = (self._second_to_time(item[0][0]),
                             self._second_to_time(item[-1][0]))
            failed = []
            for sub_item in item:
                failed.append(sub_item[1])
            correlates[time_interval] = failed
        return correlates

    def correlated_failures(self, time_threshold=120):
        correlated_in_disk_undurable = \
            self._find_correlates(self.disk_undurable_events, time_threshold)
        correlated_in_node_undurable = \
            self._find_correlates(self.node_undurable_events, time_threshold)
        correlated_in_node_unavailable = \
            self._find_correlates(self.node_unavailable_events, time_threshold)
        print correlated_in_disk_undurable

        disk_undurable_corrlates = self._correlates_process(
            correlated_in_disk_undurable)
        keys = disk_undurable_corrlates.keys()
        keys.sort()
        count = 0
        for key in keys:
            if len(disk_undurable_corrlates[key]) >= 3:
                print key, disk_undurable_corrlates[key]
            count += len(disk_undurable_corrlates[key])
        print count
        # print correlated_in_node_undurable
        # print correlated_in_node_unavailable

        return (correlated_in_disk_undurable, correlated_in_node_undurable,
                correlated_in_node_unavailable)

    def node_unavailable_ttfs(self, lifetime=False, exclude_correlated=False):
        if exclude_correlated:
            nodes = self.node_unavailable.keys()
            correlated_in_node_unavailable = \
                self._find_correlates(self.node_unavailable_events)
            correlated_failed = {}
            for one_correlate in correlated_in_node_unavailable:
                for item in one_correlate:
                    correlated_failed[item[1]] = \
                        correlated_failed.pop(item[1], []) + \
                        [item[0]]
            correlated_failed_nodes = correlated_failed.keys()

            for node in nodes:
                if node in correlated_failed_nodes:
                    for ts in correlated_failed[node]:
                        tmp = self.node_unavailable[node]
                        tmp.remove(ts)
                        if self.node_unavailable[node] == []:
                            self.node_unavailable.pop(node)
                        else:
                            self.node_unavailable[node] = tmp

        node_unavail = []
        keys = self.node_unavailable.keys()
        for key in keys:
            unavails = self.node_unavailable[key]
            length = len(unavails)
            unavails.sort()
            for i in xrange(length):
                if i == 0 and lifetime:
                    continue
                former_failed = self.start_time if i == 0 else unavails[i-1]
                ttf = float(unavails[i] - former_failed)/3600
                node_unavail.append(round(ttf, 3))
        print "length of node unavailable: ", len(keys), len(node_unavail)
        node_unavail.sort()
        return node_unavail

    def sys_unavailable_ttfs(self):
        tmp_ts = []
        node_unavail = []
        keys = self.node_unavailable.keys()
        for key in keys:
            ts_list = self.node_unavailable[key]
            tmp_ts += ts_list
        tmp_ts.sort()

        unavail_len = len(tmp_ts)
        for i in xrange(unavail_len):
            if i == 0:
                ttf = float(tmp_ts[i] - self.start_time)/3600
            else:
                ttf = float(tmp_ts[i] - tmp_ts[i-1])/3600
            node_unavail.append(round(ttf, 3))
        node_unavail.sort()
        print node_unavail
        return node_unavail

    def write_into_csv(self, failures, file_name):
        path = CSV_DIR + file_name
        with open(path, 'w') as fp:
            writer = csv.writer(fp, lineterminator='\n')
            for item in failures:
                writer.writerow([item])

    def data_generates(self, moved_report_correlated,
                       move_system_disk_failures, lifetime,
                       exclude_correlated):
        self.moved_report_correlated = moved_report_correlated
        self.move_system_disk_failures = move_system_disk_failures
        (self.disk_failures, self.node_lost, self.node_unavailable,
         self.power_outage, self.crashes, self.ignore_events) = \
            self._classfiy_events()

        disk_ttfs = self.disk_ttfs(lifetime, exclude_correlated)
        sys_disk_ttfs = self.sys_disk_undurable_ttfs()
        node_undurable_ttfs = self.node_undurable_ttfs(lifetime,
                                                       exclude_correlated)
        sys_node_undurable_ttfs = self.sys_node_undurable_ttfs()
        node_unavailable_ttfs = self.node_unavailable_ttfs(lifetime,
                                                           exclude_correlated)
        sys_node_unavailable_ttfs = self.node_unavailable_ttfs()
        return (disk_ttfs, node_undurable_ttfs, node_unavailable_ttfs,
                sys_disk_ttfs, sys_node_undurable_ttfs,
                sys_node_unavailable_ttfs)

    # Generate all the csv files we need.
    def generate_csv_files(self):
        (disk_ttfs, node_undurable_ttfs, node_unavailable_ttfs, _d1, _d2,
         _d3) = self.data_generates(False, False, False, False)
        self.write_into_csv(disk_ttfs, "disk_ttfs_raw.csv")
        self.write_into_csv(node_undurable_ttfs,
                            "node_undurable_ttfs_raw.csv")
        self.write_into_csv(node_unavailable_ttfs,
                            "node_unavailable_ttfs_raw.csv")
        (disk_ttfs, node_undurable_ttfs, node_unavailable_ttfs, _d1, _d2,
         _d3) = self.data_generates(True, True, False, False)
        self.write_into_csv(disk_ttfs,
                            "disk_ttfs_moved_sysdisk_reportcor.csv")
        self.write_into_csv(node_undurable_ttfs,
                            "node_undurable_ttfs_moved_sysdisk_reportcor.csv")
        self.write_into_csv(node_unavailable_ttfs,
                            "node_unavail_ttfs_moved_sysdisk_reportcor.csv")
        (disk_ttfs, node_undurable_ttfs, node_unavailable_ttfs, _d1, _d2,
         _d3) = self.data_generates(True, True, True, False)
        self.write_into_csv(disk_ttfs,
                            "disk_ttfs_moved_sysdisk_reportcor_lifetime.csv")
        self.write_into_csv(
            node_undurable_ttfs,
            "node_undurable_ttfs_moved_sysdisk_reportcor_lifetime.csv")
        self.write_into_csv(
            node_unavailable_ttfs,
            "node_unavail_ttfs_moved_sysdisk_reportcor_lifetime.csv")

        return

    def printAll(self):
        print "failed components: ", self.failed_components
        print "actions: ", self.actions


if __name__ == "__main__":
    te = PNNLProcess()
    te.printAll()
    # te.generate_csv_files()
    # te.node_undurable_ttfs(True, True)
    # te.node_unavailable_ttfs(True, True)
    # te.correlated_failures()
