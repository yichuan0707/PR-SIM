import ConfigParser
import os
from random import random
from simulator.utils.Log import info_logger

BASE_PATH = r"/root/PR-Sim/"
CONF_PATH = BASE_PATH + "conf"


def getConfParser(conf_file_path):
    conf = ConfigParser.ConfigParser()
    conf.read(conf_file_path)
    return conf


class Configuration(object):
    # two variables are calculated by users.
    rack_count = 3
    total_slices = 0

    def __init__(self):
        self.conf = getConfParser(CONF_PATH + os.sep + "pr-sim.conf")

        try:
            d = self.conf.defaults()
        except ConfigParser.NoSectionError:
            raise Exception("No Default Section!")

        self.total_time = int(d["total_time"])
        # total active storage in PBs
        self.total_active_storage = float(d["total_active_storage"])
        self.chunk_size = int(d["chunk_size"])
        self.chunks_per_disk = int(d["chunks_per_disk"])
        self.disks_per_machine = int(d["disks_per_machine"])
        self.machines_per_rack = int(d["machines_per_rack"])

        self.datacenters = int(d.pop("datacenters", 1))

        self.event_file = d.pop("event_file", None)

        # If n <= 15 in each stripe, no two chunks are on the same rack.
        self.num_chunks_diff_racks = 15

        self.tiered_storage = self._bool(d["tiered_storage"])
        self.heterogeneous_redundancy = self._bool(d[
            "heterogeneous_redundancy"])
        self.heterogeneous_each_layer = self._bool(d[
            "heterogeneous_each_layer"])
        self.lazy_recovery = self._bool(d["lazy_recovery"])
        self.rafi_recovery = self._bool(d["rafi_recovery"])
        self.lazy_only_available = True

        self.recovery_threshold = int(d["recovery_threshold"])
        self.bandwidth_efficient_scheme = self._bool(d[
            "bandwidth_efficient_scheme"])
        self.recovery_bandwidth_gap = int(d["recovery_bandwidth_gap"])
        self.installment_size = int(d["installment_size"])
        self.availability_counts_for_recovery = self._bool(d[
            "availability_counts_for_recovery"])
        self.max_degraded_slices = self.conf.getfloat(
            "Lazy Recovery", "max_degraded_slices")

        self.layer_num = 1
        if self.tiered_storage:
            self.tiered_mediums = d["tiered_mediums"].split(",")
            self.layer_num = len(self.tiered_mediums)

        self.redundancies = d["redundancies"]
        self.redundancies.strip()
        redun_list = self.redundancies.split(":")
        if len(redun_list) != self.layer_num:
            raise Exception("Redundancy schemes not match system tiers!")

        # {layer_id:[(RS, n, k), (LRC, n, k, l)], ...}
        self.redundancies_dict = {}
        heterogeneous_redun_flag = False
        for i, item in enumerate(redun_list):
            # item.strip()
            layer_redun = item[1:-1].split(",")
            if len(layer_redun) >= 2:
                heterogeneous_redun_flag = True
            self.redundancies_dict[i] = [self.codingParams(t)
                                         for t in layer_redun]
        if self.heterogeneous_redundancy != heterogeneous_redun_flag:
            raise Exception("Redundancies is not sufficient!")

        if self.layer_num == 1 and not self.heterogeneous_redundancy:
            self.n = self.redundancies_dict.values()[0][0][1]
            self.k = self.redundancies_dict.values()[0][0][2]

        # check recovery settings.
        if self.lazy_recovery:
            if not self.conf.has_section("Lazy Recovery"):
                raise Exception("Lack of Lazy Recovery Settings!")
            ava_to_dt = self.conf.get(
                "Lazy Recovery", "availability_to_durability_threshold")

            self.availability_to_durability_threshold = \
                self._commaParser(ava_to_dt.strip())
            recov_prob = self.conf.get("Lazy Recovery", "recovery_probability")
            self.recovery_probability = self._commaParser(recov_prob.strip())

        if self.rafi_recovery:
            if not self.conf.has_section("RAFI Recovery"):
                raise Exception("Lack of RAFI Recovery Settings!")
            pass

    def _bool(self, string):
        if string.lower() == "true":
            return True
        elif string.lower() == "false":
            return False
        else:
            raise Exception("String must be 'true' or 'false'!")

    # extract data seperated by ",".
    def _commaParser(self, string):
        return [int(item) for item in string.split(",")]

    # i.e. given string is 'RS_n_k', return ['RS', n, k]
    def codingParams(self, string):
        params = string.split("_")
        return tuple([params[0]] + [int(item) for item in params[1:]])

    # "True" means events record to file, and vice versa.
    def eventToFile(self):
        return self.event_file is not None

    def getAvailableLazyThreshold(self, time_since_failed):
        threshold_gap = self.n - 1 - self.recovery_threshold
        length = len(self.availability_to_durability_threshold)
        index = 0
        for i in xrange(length):
            if self.availability_to_durability_threshold[i] < \
               time_since_failed and \
               self.availability_to_durability_threshold[i+1] >= \
               time_since_failed:
                if i > 0:
                    index = i
                break
        threshold_increment = threshold_gap * \
            (1 if random() < self.recovery_probability[i] else 0)
        return self.recovery_threshold + threshold_increment

    def returnLayerNum(self):
        return self.layer_num

    def returnAll(self):
        d = {"total_time": self.total_time,
             "total_active_storage": self.total_active_storage,
             "chunk_size": self.chunk_size,
             "chunks_per_disk": self.chunks_per_disk,
             "disks_per_machine": self.disks_per_machine,
             "machines_per_rack": self.racks,
             "datacenters": self.datacenters,
             "event_file": self.event_file,
             "tiered_storage": self.tiered_storage,
             "heterogeneous_redundancy": self.heterogeneous_redundancy,
             "heterogeneous_each_layer": self.heterogeneous_each_layer,
             "redundances": self.redundancies,
             "lazy_recovery": self.lazy_recovery,
             "rafi_recovery": self.rafi_recovery,
             "recovery_threshold": self.recovery_threshold,
             "bandwidth_efficient_scheme": self.bandwidth_efficient_scheme,
             "recovery_bandwidth_gap": self.recovery_bandwidth_gap,
             "installment_size": self.installment_size,
             "availability_counts_for_recovery":
             self.availability_counts_for_recovery}

        if self.tiered_storage:
            d["tiered_mediums"] = self.tiered_mediums
        if self.lazy_recovery:
            d["max_degraded_slices"] = self.max_degraded_slices
            d["availability_to_durability_threshold"] \
                = self.availability_to_durability_threshold
            d["recovery_probability"] = self.recovery_probability
        if self.rafi_recovery:
            pass

        return d

    def printAll(self):
        info_logger.info("Configuration:\t total_time: " + str(self.total_time)
                         + ", chunks per disk: " + str(self.chunks_per_disk)
                         + ", chunks per machine: "
                         + str(self.chunks_per_disk*self.disks_per_machine)
                         + ", n: " + str(self.n) + "k: " + str(self.k)
                         + ", lazy recovery: " + str(self.lazy_recovery)
                         + ", recovery threshold: "
                         + str(self.recovery_threshold)
                         + ", recovery bandwidth cap: "
                         + str(self.recovery_bandwidth_gap)
                         + ", installment size: " + str(self.installment_size)
                         + ", lazy only available: "
                         + str(self.lazy_only_available)
                         + ", availablity counts for recovery: "
                         + str(self.availability_counts_for_recovery)
                         + ", num chunks diff racks: "
                         + str(self.num_chunks_diff_racks)
                         + ", chunk size: " + str(self.chunk_size) + "MB"
                         + ", total_active_storage: "
                         + str(self.total_active_storage)
                         + ", rack count: " + str(Configuration.rack_count)
                         + ", disks per machine: "
                         + str(self.disks_per_machine)
                         + ", bandwidth efficient reconstruction: "
                         + str(self.bandwidth_efficient_scheme)
                         + ", machines per rack: "
                         + str(self.machines_per_rack)
                         + ", datacenters:" + str(self.datacenters)
                         + ", event file:" + str(self.event_file)
                         + ", tiered storage:" + str(self.tiered_storage)
                         + ", heterogeneous redundancy:"
                         + str(self.heterogeneous_redundancy)
                         + ", heterogeneous each layer:"
                         + str(self.heterogeneous_each_layer)
                         + ", redundances:" + str(self.redundancies)
                         + ", rafi recovery:" + str(self.rafi_recovery)
                         + ", availability_counts_for_recovery:"
                         + str(self.availability_counts_for_recovery) + "\n")


if __name__ == "__main__":
    conf = Configuration()
    print conf.printAll()
