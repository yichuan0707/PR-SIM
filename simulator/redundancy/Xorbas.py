from copy import deepcopy
from simulator.redundancy.LocalRepairCode import LRC


# XORBAS can be treated as special LRC, which 'm0=1', and all parity blocks
# make up with a new group, which can repair one failure with optimal repair
# cost.
# And in XORBAS, we usually have 'l + m1 = k/l + m0'
class XORBAS(LRC):

    def __init__(self, params):
        params['m0'] = 1
        super(XORBAS, self).__init__(params)

    def is_repairable(self, state):
        parity_group = state[-(self.m1 + self.ll):]
        new_state = deepcopy(state)
        if parity_group.count(0) == 1:
            parity_recovery_id = parity_group.index(0)
            new_state[self.k + parity_recovery_id] += 1
        return super(XORBAS, self).is_repairable(new_state)

    def stateRepairCost(self, state):
        if not self.is_repairable(state):
            return -1

        # Only difference is tha parity group with optimal repair.
        local_groups = self._extract_local(state)
        parity_group = state[self.k:]
        global_group = state[-self.m1:]
        local_fail_group_id = -1
        for i, item in enumerate(local_groups):
            fail = item.count(0)
            if fail > 0 and fail <= self.m0:
                recovered_id = item.index(0)
                item[recovered_id] += 1
                return (self.optimalRepairCost(),
                        self._combine(local_groups) + global_group)
            elif fail == 0:
                continue
            else:
                local_fail_group_id = i

        if parity_group.count(0) == 1:
            recovered_id = parity_group.index(0)
            parity_group[recovered_id] += 1
            return (self.optimalRepairCost(),
                    state[:self.k] + parity_group)

        if local_fail_group_id == -1 and global_group.count(0):
            recovered_id = global_group.index(0)
            global_group[recovered_id] += 1
            return (self.normalRepairCost(),
                    state[:-self.m1] + global_group)
        else:
            fail_group = local_groups[local_fail_group_id]
            recovered_id = fail_group.index(0)
            fail_group[recovered_id] += 1
            return (self.normalRepairCost(),
                    self._combine(local_groups) + global_group)

    def stateParallRepairCost(self, state):
        if not self.is_repairable(state):
            return -1

        local_groups = self._extract_local(state)
        failure_groups = 0
        local_failures = 0
        for item in local_groups:
            fail = item.count(0)
            if fail > self.m0:
                failure_groups = -1
                break
            else:
                failure_groups += 1
                local_failures += fail

        # Only difference with LRC locates in single global parity recovery.
        if failure_groups == 1 and state[-self.m1:].count(0) == 0:
            return self.optimalRepairCost() + local_failures - 1
        elif failure_groups == 0 and state[-self.m1].count(0) == 1:
            return self.optimalRepairCost()
        else:
            return self.normalRepairCost() + state.count(0) - 1
        pass


if __name__ == "__main__":
    lrc = XORBAS({'k': 6, 'l': 2, 'm0': 1, 'm1': 2})
    print lrc.is_MDS
    print lrc.deviceStorageCost()
    print lrc.systemStorageCost()
    print lrc.optimalRepairCost()
    print lrc.normalRepairCost()

    state = [1, 1, 1, 1, 0, 1, 1, 0, 1, 1]
    print lrc.is_repairable(state)
    print lrc.stateRepairCost(state)
    print lrc.stateParallRepairCost(state)
