import sys
import logging
import logging.config
import csv
from time import strftime, time
from math import ceil

from simulator.utils.Log import info_logger, error_logger
from simulator.Configuration import Configuration
from simulator.unit.Machine import Machine
from simulator.failure.WeibullGenerator import WeibullGenerator
from simulator.eventHandler.RandomDistributeEventHandler import \
    RandomDistributeEventHandler

from simulator.utils.XMLParser import XMLParser
from simulator.EventQueue import EventQueue
from simulator.Event import Event
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
        # I think the rack can't be filled up, or Exception will be raised during distributeSlice.
        actual_storage_rack = chunks_per_rack*conf.chunk_size/1024.0*5.0/6.0  # in GB
        print "actual_storage_rack:", actual_storage_rack
        if actual_storage_rack <= 0:
            raise Exception("too many slices generated, overflow!")

        sclaing_flag = True
        scaling_capacity = [1, 2]
        scaling_times = len(scaling_capacity)
        if scaling_times == 1:
            scaling_flag = False
            raise Exception("No scaling setting found.")

        scaling_timestamps = [conf.total_time/scaling_times*i for i in xrange(scaling_times)]
        print scaling_timestamps

        un_available_count = 0
        un_durable_count = 0
        if conf.event_file is not None:
            # :(colon) is the reserved characters in Windows filename.
            # so, we could not use "%H:%M:%S" in the following.
            ts = strftime("%Y%m%d.%H.%M.%S")
            conf.event_file += '-' + ts
            info_logger.info("Events output to: " + conf.event_file)
            res_file = "/root/PR-Sim/log/durability-" + ts
            info_logger.info("Durabilities output to: " + res_file)

        last_racks = 0
        undur_unavail = []
        racks_start_times = {}
        for i, ts in enumerate(scaling_timestamps):
            # in GBs
            total_storage_overheads = scaling_capacity[i]*1024*1024.0*n/k
            print "total_storage_overheads:", total_storage_overheads

            # we regard all disks in rack have the usage 75%.
            racks = int(ceil(total_storage_overheads/actual_storage_rack))
            racks_start_times[ts] = racks - last_racks

            # give the right rack count to Configuration
            Configuration.rack_count = racks
            print "rack count:", Configuration.rack_count

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

            iteration_count = int(sys.argv[1])
            for i in xrange(iteration_count):
                xml = XMLParser(1)
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

                events = EventQueue()
                total_racks = root.getChildren()[0].getChildren()
                for j, rack in enumerate(total_racks):
                    period = conf.total_time/scaling_times
                    start_times = racks_start_times.keys()
                    start_times.sort()
                    racks_num = [racks_start_times[item] for item in start_times]
                    rack_id_thresholds = [racks_num[0]] + [racks_num[i]+racks_num[i+1] for i in xrange(len(racks_num)-1)]

                    for x, rack_id in enumerate(rack_id_thresholds):
                        if j < rack_id:
                            rack.generateEvents(events, ts - x*period, ts-(x-1)*period, True)


                if True:  # conf.event_file is not None:
                    events.printAll(conf.event_file,
                                    "Iteration number: " + str(i))
                # else:
                conf.printAll()
                handler = RandomDistributeEventHandler()
                handler.start(root, total_slices, total_disks)
                error_logger.error("Starting simulation ")


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

            # Record undurability and unavailability in csv files.
            undurability = (float(handler.undurable_slice_count) /
                            len(handler.available_count)) * 100
            unavail_time = 0.0
            unavailables = handler.unavailable_durations.keys()
            print "number of unavailable slices:", len(unavailables)
            for item in unavailables:
                for ts_duration in handler.unavailable_durations[item]:
                    unavail_time += ts_duration[1] - ts_duration[0]
            unavailability = (unavail_time/(conf.total_slices*conf.total_time)) * 100
            print "unavailability:", unavailability
            undur_unavail.append([round(undurability, 5), round(unavailability, 5)])

            last_racks = racks

        if True:  # conf.event_file is None:
            info_logger.info("avg unavailable = %.5f" %
                             (float(un_available_count)/iteration_count))
            info_logger.info("avg undurable = %.5f" %
                             (float(un_durable_count)/iteration_count))

        with open(res_file, 'w') as fp:
            writer = csv.writer(fp, lineterminator='\n')
            for item in undur_unavail:
                writer.writerow(item)



if __name__ == "__main__":
    st_time = time()
    t = Test()
    t.main()
    end_time = time()
    print "the execute time is %.2f minutes" % ((end_time - st_time)/60)
