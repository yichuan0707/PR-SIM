from simulator.Unit import Unit
from simulator.Event import Event


class Rack(Unit):

    def __init__(self, name, parent, parameters):
        super(Rack, self).__init__(name, parent, parameters)
        # If True, rack failure and recovery events will be generated
        # but ignored(neither handled nor written to file).
        self.fast_forward = bool(parameters.get("fast_forward"))

    def generateEvents(self, result_events, start_time, end_time, reset):
        current_time = start_time
        last_recover_time = start_time

        if self.failure_generator is None:
            for u in self.children:
                u.generateEvents(result_events, start_time, end_time, True)
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
            if self.fast_forward:
                fail_event.ignore = True

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
            if self.fast_forward:
                result_events.addEvent(Event(Event.EventType.Recoverd,
                                       current_time, self, ignore=True))
            else:
                result_events.addEvent(Event(Event.EventType.Recovered,
                                       current_time, self))
            last_recover_time = current_time
