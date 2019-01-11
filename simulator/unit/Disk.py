from simulator.Unit import Unit
from numpy import isnan, isinf
from simulator.Event import Event


class Disk(Unit):

    def addEventGenerator(self, generator):
        self.latent_error_generator = None
        if generator.getName() == "latentErrorGenerator":
            self.latent_error_generator = generator
        else:
            super(Disk, self).addEventGenerator(generator)

    def generateEvents(self, result_events, start_time, end_time, reset):
        current_time = start_time
        last_recover_time = start_time

        if self.children is not None and len(self.children) != 0:
            raise Exception("Disk should not have any children!")

        while True:
            self.failure_generator.reset(current_time)
            failure_time = self.failure_generator.generateNextEvent(
                current_time)
            current_time = failure_time
            if current_time > end_time:
                if self.latent_error_generator is None:
                    break
                self.generateLatentErrors(result_events, last_recover_time,
                                          end_time)
                break
            fail_event = Event(Event.EventType.Failure, current_time, self)
            result_events.addEvent(fail_event)

            if self.latent_error_generator is not None:
                self.generateLatentErrors(result_events, last_recover_time,
                                          current_time)
            self.recovery_generator.reset(current_time)
            recovery_time = self.recovery_generator.generateNextEvent(
                current_time)
            assert (recovery_time > failure_time)
            fail_event.next_recovery_time = recovery_time
            current_time = recovery_time
            if current_time > end_time:
                result_events.addEvent(Event(Event.EventType.Recovered,
                                             current_time, self))
                break
            result_events.addEvent(Event(Event.EventType.Recovered,
                                         current_time, self))
            last_recover_time = current_time

    def generateRecoveryEvent(self, result_events, failure_time, end_time):
        if end_time < 0 or failure_time < 0:
            raise Exception("end time or failure time is negative")
        if isinf(failure_time) or isnan(failure_time):
            raise Exception("start time = Inf or NAN")
        if isinf(end_time) or isnan(end_time):
            raise Exception("end time = Inf or NaN")

        self.recovery_generator.reset(failure_time)
        recovery_time = self.recovery_generator.generateNextEvent(failure_time)
        # if recovery falls later than the end time (which is the time of the
        # next failure of the higher-level component we just co-locate the
        # recovery with the failure because the data will remain unavailable
        # in either case)
        if recovery_time > end_time:
            recovery_time = end_time
        self.last_recovery_time = recovery_time
        if self.last_recovery_time < 0:
            raise Exception("recovery time is negative")
        result_events.addEvent(Event(Event.EventType.Recovered, recovery_time,
                                     self))
        return recovery_time

    def generateLatentErrors(self, result_events, start_time, end_time):
        self.latent_error_generator.reset(start_time)
        current_time = start_time
        while True:
            latent_error_time = self.latent_error_generator.generateNextEvent(
                current_time)
            current_time = latent_error_time
            if current_time > end_time:
                break
            result_events.addEvent(Event(Event.EventType.LatentDefect,
                                         current_time, self))
