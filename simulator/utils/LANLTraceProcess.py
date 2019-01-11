import csv
import os
from time import mktime, strptime, localtime, strftime

BASE_PATH = os.path.dirname(os.path.dirname(os.getcwd()))
TRACE_DIR = BASE_PATH + os.sep + "traces" + os.sep
LANL_TRACE_PATH = TRACE_DIR + "LANL.csv"
CSV_DIR = TRACE_DIR + "csv" + os.sep


class LANLProcess(object):
    trace_path = LANL_TRACE_PATH
    tmp_dir = TRACE_DIR + "tmp" + os.sep
    not_given_start_production = ['7', '17', '22', '24']

    def __init__(self):
        self.sub_sys_info = {}
        self.failure_type = []
        self._divide_into_system()
        self.data_lost_types = ['disk io', 'disk cabinet', 'disk drive',
                                'san disk drive', 'scsi drive',
                                'scsi controller', 'scsi adapter card',
                                'fibre raid controller', 'fibre driver',
                                'fibre raid lcc card', 'system controller',
                                'pci scsi controller', 'scsi card',
                                'unresloved']

    def _time_to_second(self, format_time):
        return mktime(strptime(format_time, '%m/%d/%Y %H:%M'))

    def _time_to_second2(self, format_time):
        return mktime(strptime(format_time, '%m/%d/%Y'))

    def _second_to_time(self, seconds):
        return strftime('%m/%d/%Y %H:%M', localtime(seconds))

    # divide the full LANL trace into system traces, put the trace file
    # under tmp_dir and systm id as the filename.
    def _divide_into_system(self):
        with open(LANLProcess.trace_path, 'r') as fp:
            last_sys_id = 0
            sys_id = 0
            sys_info = []
            count = 0
            for line in fp:
                count += 1
                if count < 3:
                    continue
                info = line.split(",")
                sys_id = info[0]
                if sys_id not in self.sub_sys_info.keys():
                    self.sub_sys_info[sys_id] = info[1:3]
                if last_sys_id == 0:
                    last_sys_id = sys_id
                elif last_sys_id != sys_id:
                    with open(LANLProcess.tmp_dir+last_sys_id, 'w') as \
                         tmp:
                        for item in sys_info:
                            tmp.write(','.join(item)+"\n")
                    sys_info = []
                    last_sys_id = sys_id
                else:
                    pass
                # Useful info: node_id, node_install_time,
                #  node_production_time, node_decomp_time,
                #  failure_start_time, failure_end_time, duration,
                #  failure type and corresponding infomation
                if sys_id in LANLProcess.not_given_start_production:
                    useful_info = [info[5]] + info[7:10] + info[16:25]
                else:
                    failed_node_id = info[5]
                    # use start production time as start time, not install
                    # time(info[7])
                    start_production_ts = self._time_to_second2(info[8])
                    failed_ts = self._time_to_second(info[16])
                    duration = info[18]
                    ttf = str(failed_ts - start_production_ts)
                    if ttf < 0.0:
                        print info
                    msg = " ".join(info[19:25])
                    msg = msg.strip()
                    msg = msg.lower()
                    if msg.find("disk io") != -1:
                        msg = "disk io"
                    if msg.strip() not in self.failure_type:
                        self.failure_type.append(msg)
                    # format: node_id, ttf from production time, duratiom, msg
                    useful_info = [failed_node_id, ttf, duration, msg]
                sys_info.append(useful_info)

            with open(LANLProcess.tmp_dir+sys_id, 'w') as tmp:
                for item in sys_info:
                    tmp.write(",".join(item)+"\n")

        # print self.sub_sys_info
        # print self.failure_type
        # print len(self.failure_type)

    # sys_id is character, '2', not int 2.
    def _classfiy_events(self, sys_id=None):
        node_lost = {}
        node_unavailable = {}
        power_outage = {}
        power_related = {}
        lse_related = {}
        count = 0

        if sys_id is None:
            if os.listdir(LANLProcess.tmp_dir) == []:
                self._divide_into_system()
            for item in os.listdir(LANLProcess.tmp_dir):
                if item in LANLProcess.not_given_start_production:
                    continue
                (sub_node_lost, sub_node_unavailable, sub_power_outage,
                 sub_power_related, sub_lse_related) = \
                    self._classfiy_events(item)
                node_lost.update(sub_node_lost)
                node_unavailable.update(sub_node_unavailable)
                power_outage.update(sub_power_outage)
                power_related.update(sub_power_related)
                lse_related.update(sub_lse_related)
        else:
            filepath = LANLProcess.tmp_dir + sys_id
            with open(filepath, 'r') as fp:
                for line in fp:
                    info = line[:-1].split(",")
                    full_node_id = sys_id + "_" + info[0]
                    # ttfs start from production time in hours.
                    failure_start = round(float(info[1])/3600, 3)
                    duration = round(float(info[2])/60, 3)

                    node_unavailable[full_node_id] = \
                        node_unavailable.pop(full_node_id, []) + \
                        [(failure_start, duration)]
                    if info[-1] in self.data_lost_types:
                        count += 1
                        node_lost[full_node_id] = \
                            node_lost.pop(full_node_id, []) \
                            + [(failure_start, duration)]
                    else:
                        if info[-1].find("power outage") != -1:
                            power_outage[full_node_id] = \
                                power_outage.pop(full_node_id, []) + \
                                [(failure_start, duration)]
                        if info[-1].find("power") != -1:
                            power_related[full_node_id] = \
                                power_related.pop(full_node_id, []) + \
                                [(failure_start, duration)]
                        if info[-1].find("dst") != -1:
                            lse_related[full_node_id] = \
                                lse_related.pop(full_node_id, []) + \
                                [(failure_start, duration)]
        return (node_lost, node_unavailable, power_outage, power_related,
                lse_related)

    def node_unavailable_ttfs(self, sys_id=None):
        if isinstance(sys_id, int):
            sys_id = str(sys_id)

        ttfs = []
        _d1, node_unavailable, _d2, _d3, _d4 = self._classfiy_events(sys_id)
        keys = node_unavailable.keys()
        for key in keys:
            ts_list = node_unavailable[key]
            for i in xrange(len(ts_list)):
                ttf = ts_list[0][0] if i == 0 else (ts_list[i][0] -
                                                    ts_list[i-1][0])
                ttfs.append(round(ttf, 3))
        ttfs.sort()

        return ttfs

    def sys_unavailable_ttfs(self, sys_id=None):
        if isinstance(sys_id, int):
            sys_id = str(sys_id)

        ttfs = []
        if sys_id is None:
            if os.listdir(LANLProcess.tmp_dir) == []:
                self._divide_into_system()
            for item in os.listdir(LANLProcess.tmp_dir):
                if item in LANLProcess.not_given_start_production:
                    continue
                sub_ttfs = self.sys_unavailable_ttfs(item)
                ttfs += sub_ttfs
        else:
            tmp = []
            _d1, node_unavailable, _d2, _d3, _d4 = \
                self._classfiy_events(sys_id)
            keys = node_unavailable.keys()
            for key in keys:
                ts_list = node_unavailable[key]
                for item in ts_list:
                    tmp.append(item[0])
            tmp.sort()

            for i in xrange(len(tmp)):
                ttf = tmp[0] if i == 0 else (tmp[i] - tmp[i-1])
                ttfs.append(round(ttf, 3))
        ttfs.sort()

        return ttfs

    def node_undurable_ttfs(self, sys_id=None):
        if isinstance(sys_id, int):
            sys_id = str(sys_id)

        ttfs = []
        node_lost, _d1, _d2, _d3, _d4 = self._classfiy_events(sys_id)
        keys = node_lost.keys()
        for key in keys:
            ts_list = node_lost[key]
            for i in xrange(len(ts_list)):
                ttf = ts_list[i][0] if i == 0 else (ts_list[i][0] -
                                                    ts_list[i-1][0])
                ttfs.append(round(ttf, 3))
        ttfs.sort()

        return ttfs

    def sys_undurable_ttfs(self, sys_id=None):
        if isinstance(sys_id, int):
            sys_id = str(sys_id)

        ttfs = []
        if sys_id is None:
            if os.listdir(LANLProcess.tmp_dir) == []:
                self._divide_into_system()
            for item in os.listdir(LANLProcess.tmp_dir):
                if item in LANLProcess.not_given_start_production:
                    continue
                sub_ttfs = self.sys_undurable_ttfs(item)
                ttfs += sub_ttfs
        else:
            tmp = []
            node_lost, _d1, _d2, _d3, _d4 = self._classfiy_events(sys_id)

            keys = node_lost.keys()
            for key in keys:
                ts_list = node_lost[key]
                for item in ts_list:
                    tmp.append(item[0])
            tmp.sort()

            for i in xrange(len(tmp)):
                ttf = tmp[0] if i == 0 else (tmp[i] - tmp[i-1])
                ttfs.append(round(ttf, 3))
        ttfs.sort()

        return ttfs

    def printAll(self):
        pass

    def test_failure_type(self):
        disk_type = []
        drive_type = []
        scsi_type = []
        controller_type = []
        fiber_type = []
        for item in self.failure_type:
            if item.find("disk") != -1:
                disk_type.append(item)
            if item.find("drive") != -1:
                drive_type.append(item)
            if item.find("scsi") != -1:
                scsi_type.append(item)
            if item.find("controller") != -1:
                controller_type.append(item)
            if item.find("fiber") != -1:
                fiber_type.append(item)

        print "disk:", disk_type
        print "drive:", drive_type
        print "scsi:", scsi_type
        print "controller:", controller_type
        print "fiber:", fiber_type

    def write_into_csv(self, failures, file_name):
        path = CSV_DIR + file_name
        with open(path, 'w') as fp:
            writer = csv.writer(fp, lineterminator='\n')
            for item in failures:
                writer.writerow([item])

    def data_generates(self, sys_id=None):
        if isinstance(sys_id, int):
            sys_id = str(sys_id)

        self.write_into_csv(self.node_undurable_ttfs(sys_id),
                            "LANL_node_undurable_ttfs.csv")
        self.write_into_csv(self.sys_undurable_ttfs(sys_id),
                            "LANL_sys_node_undurable_ttfs.csv")
        self.write_into_csv(self.node_unavailable_ttfs(sys_id),
                            "LANL_node_unavailable_ttfs.csv")
        self.write_into_csv(self.sys_unavailable_ttfs(sys_id),
                            "LANL_sys_node_unavailable_ttfs.csv")

        # statistic about others
        _d1, _d2, power_outage, power_related, lse_related = \
            self._classfiy_events(sys_id)
        po_count = 0
        pl_count = 0
        lse_count = 0
        for key in power_outage.keys():
            po_count += len(power_outage[key])
        for key in power_related.keys():
            pl_count += len(power_related[key])
        for key in lse_related.keys():
            lse_count += len(lse_related[key])
        print "power outage node:%d, numbers:%d" % (len(power_outage.keys()),
                                                    po_count)
        print "power related node:%d, numbers:%d" % (len(power_related.keys()),
                                                     pl_count)
        print "lse node:%d, numbers:%d" % (len(lse_related.keys()), lse_count)


if __name__ == "__main__":
    te = LANLProcess()
    te.data_generates()
