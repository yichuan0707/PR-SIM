import sys
import logging
import logging.config
from time import strftime
from math import ceil

from simulator.utils.Log import info_logger, error_logger
from simulator.Configuration import Configuration
from simulator.unit.Machine import Machine
from simulator.failure.WeibullGenerator import WeibullGenerator
from simulator.eventHandler.RandomDistributeEventHandler import \
    RandomDistributeEventHandler

from simulator.utils.XMLParser import XMLParser
from simulator.EventQueue import EventQueue
from simulator.Result import Result

conf = Configuration()


class Test(object):

    def __init__(self):
        self.logger = logging.getLogger("infoLogger")
        # self.components = ("DataCenter", "Rack", "Machine", "Disk")
        self.units = []

    def getMachineFailureGeneratorRate(self, root):
        m = root.getChildren()[0].getChildren()[0].getChildren()[0]
        if isinstance(m.getFailureGenerator(), WeibullGenerator):
            return (m.getFailureGenerator()).getRate()
        return -1

    def main(self):
        n, k = conf.n, conf.k
        total_active_storage = conf.total_active_storage
        disks_per_machine = conf.disks_per_machine
        machines_per_rack = conf.machines_per_rack

        events_handled = 0
        recovery_threshold = conf.recovery_threshold
        if recovery_threshold == n:
            recovery_threshold -= 1
            error_logger.error("Recovery threshold = chunks per slice - making \
                                it one less: " + str(recovery_threshold))
        if recovery_threshold < n - 1 and not conf.lazy_recovery:
            error_logger.error("Recovery threshold is less than # chunks per \
                                slice but lazy recovery is disabled - \
                                enabling")

        chunks_per_machine = disks_per_machine*conf.chunks_per_disk
        chunks_per_rack = machines_per_rack*chunks_per_machine
        actual_storage_rack = chunks_per_rack*conf.chunk_size/1024.0  # in GB
        print "actual_storage_rack:", actual_storage_rack
        if actual_storage_rack <= 0:
            raise Exception("too many slices generated, overflow!")

        # in GBs
        total_storage_overheads = total_active_storage*1024*1024.0*n/k
        print "total_storage_overheads:", total_storage_overheads

        # we regard all disks in rack have the usage 75%.
        racks = int(ceil(total_storage_overheads/actual_storage_rack))

        # give the right rack count to Configuration
        # Configuration.rack_count = racks

        # small number just for test!!
        Configuration.rack_count = 3
        conf.num_chunks_diff_racks = 1

        # One rack is a failure domain, make sure data spreading across racks
        # if racks <= conf.num_chunks_diff_racks*20/10:
        #     raise Exception("Number of racks too small, adjust num chunks \
        #                      diff racks")

        total_disks = Configuration.rack_count * conf.machines_per_rack * \
            conf.disks_per_machine

        total_slices = int(ceil(total_storage_overheads * 1024.0 /
                                (conf.chunk_size*n)))
        Configuration.total_slices = total_slices
        info_logger.info("total slices=" + str(total_slices) +
                         " disk count=" + str(total_disks) +
                         " total storage=" + str(total_storage_overheads) +
                         "GB" + " disk size=" +
                         str(conf.chunk_size*conf.chunks_per_disk/1024) + "GB")

        un_available_count = 0
        un_durable_count = 0
        if conf.event_file is not None:
            # :(colon) is the reserved characters in Windows filename.
            # so, we could not use "%H:%M:%S" in the following.
            ts = strftime("%Y%m%d.%H.%M.%S")
            conf.event_file += '-' + ts
            info_logger.info("Events output to: " + conf.event_file)

        iteration_count = int(sys.argv[1])
        for i in xrange(iteration_count):
            layer_num = conf.returnLayerNum()
            for i in xrange(1, layer_num + 1):
                xml = XMLParser(i)
                self.units = xml.readFile()
                root = self.units[0]
                if Machine.fail_fraction != 0:
                    rate = self.getMachineFailureGeneratorRate(root)
                if rate != -1 and rate != 0:
                    total_machines = conf.machines_per_rack * \
                        Configuration.rack_count
                    all_machine_failure_per_hour = total_machines/rate
                    permanent_machines_per_hour = Machine.fail_fraction * \
                        total_machines/(24*30)
                    Machine.fail_fraction = permanent_machines_per_hour / \
                        all_machine_failure_per_hour

                # need to check all the disks are instance of
                # DiskWithScrubbing?
                # Yes, all disks are instance of DiskWithScrubbing.
                events = EventQueue()
                root.generateEvents(events, 0, conf.total_time, True)

                if True:  # conf.event_file is not None:
                    events.printAll(conf.event_file,
                                    "Iteration number:  " + str(i))
                # else:
                    conf.printAll()
                    handler = RandomDistributeEventHandler()
                    handler.start(root, total_slices, total_disks)
                    error_logger.error("Starting simulation ")

                    # print slices locations to file for debugging.
                    # with open("locations", "w") as fp:
                    #     for i, s in enumerate(handler.slice_locations):
                    #         msg = str(i)
                    #         for disk in s:
                    #             msg += "  " + disk.getFullName()
                    #         msg += "\n"
                    #         fp.write(msg)

                    # Event handling
                    e = events.removeFirst()
                    while e is not None:
                        handler.handleEvent(e, events)
                        e = events.removeFirst()
                        events_handled += 1

                    result = handler.end()
                    info_logger.info(result.toString())
                    info_logger.info("Events handled: %d" % events_handled)
                    un_available_count += Result.unavailable_count
                    un_durable_count += Result.undurable_count

        if True:  # conf.event_file is None:
            info_logger.info("avg unavailable = %.5f" %
                             (float(un_available_count)/iteration_count))
            info_logger.info("avg undurable = %.5f" %
                             (float(un_durable_count)/iteration_count))


if __name__ == "__main__":
    t = Test()
    t.main()
