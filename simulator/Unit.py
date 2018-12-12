from abc import ABCMeta
from simulator.Metadata import Metadata
from simulator.Event import Event


class Unit:
    __metaclass__ = ABCMeta
    unit_count = 0

    def __init__(self, name, parent, parameters):
        self.children = []
        self.parent = parent
        self.name = name
        self.failure_generator = None
        self.recovery_generator = None
        self.meta = Metadata()

        self.id = Unit.unit_count
        self.last_failure_time = 0
        self.last_bandwidth_need = 0

        Unit.unit_count += 1

    def setLastFailureTime(self, ts):
        self.last_failure_time = ts

    def getLastFailureTime(self):
        return self.last_failure_time

    def setLastBandwidthNeed(self, bw):
        self.last_bandwidth_need = bw

    def getLastBandwidthNeed(self):
        return self.last_bandwidth_need

    # unit must be a instance.
    def addChild(self, unit):
        self.children.append(unit)

    def getChildren(self):
        return self.children

    def getParent(self):
        return self.parent

    def getMetadata(self):
        return self.meta

    def getID(self):
        return self.id

    def addEventGenerator(self, generator):
        if generator.getName() == "failureGenerator":
            self.failure_generator = generator
        elif generator.getName() == "recoveryGenerator":
            self.recovery_generator = generator
        else:
            raise Exception("Unknown generator" + generator.getName())

    def generateEvents(self, result_events, start_time, end_time, reset):
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for unit in self.children:
                unit.generateEvents(result_events, start_time, end_time, reset)
            return

        while True:
            if reset:
                self.failure_generator.reset(current_time)

            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                for u in self.children:
                    u.generateEvents(result_events, last_recover_time,
                                     end_time, True)
                break
            fail_event = Event(Event.EventType.Failure, current_time, self)
            result_events.addEvent(fail_event)
            for u in self.children:
                u.generateEvents(result_events, last_recover_time,
                                 current_time, True)

            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            current_time = recovery_time
            fail_event.next_recovery_time = recovery_time

            if current_time > end_time:
                break

            result_events.addEvent(Event(Event.EventType.Recovered,
                                         current_time, self))
            last_recover_time = current_time

    def toString(self):
        if self.parent is None:
            return self.name
        else:
            return self.parent.toString() + '.' + self.name

    def printAll(self):
        print '--' + self.name
        for unit in self.children:
            print '--',
            unit.printAll()

    def getFullName(self):
        full_name = self.name
        pa = self.getParent()
        while pa is not None:
            full_name = pa.name + "." + full_name
            pa = pa.getParent()

        return full_name
