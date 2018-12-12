from simulator.Configuration import Configuration
from simulator.redundancy.base import Base


class LRC(Base):

    def __init__(self, params):
        self.k = params['k']
        self.ll = params['l']
        self.m0 = params['m0']
        self.m1 = params['m1']
        self._check()

        self.n = self.k + self.ll * self.m0 + self.m1

        conf = Configuration()
        self.chunk_size = conf.chunk_size

    def _check(self):
        if self.k < 0 or self.ll < 0 or self.m0 < 0 or self.m1 < 0:
            raise Exception("All coding parameters must be positive integer!")

    # Get local groups list.
    def _extract_local(self, state):
        b = self.k/self.ll
        local_groups = [state[x * b: (x + 1) * b] +
                        state[(self.k + x * self.m0):
                        (self.k + (x + 1) * self.m0)]
                        for x in xrange(self.ll)]
        return local_groups

    # Reverse function to _extrace_local()
    def _combine(self, local_groups):
        datas = []
        local_parities = []

        b = self.k/self.ll
        for item in local_groups:
            datas += item[:b]
            local_parities += item[b:]

        return datas + local_parities

    @property
    def is_MDS(self):
        return False

    def optimalRepairCost(self):
        return float(self.k)/float(self.ll)

    # state format: [data block states, local parity states in group1,
    # local parity in group 2, global parity blocks]
    def is_repairable(self, state):
        if len(state) != self.n:
            raise Exception("State Length Error!")

        avails = state.count(1)

        if avails == self.n:
            raise Exception("No need to repair!")

        if avails < self.k:
            return False

        local_groups = self._extract_local(state)

        # check global group after local repairs.
        global_group = []
        b = self.k/self.ll
        # avail_equ means equation number we can use for recovery.
        avail_equ = 0
        loss_amount = 0
        for group in local_groups:
            if group.count(0) <= self.m0:
                global_group += [1 for item in xrange(b)]
            else:
                avail_equ += min(group.count(1), self.m0)
                loss_amount += group.count(0)
                global_group += group[:b]

        global_parity = state[-self.m1:]
        avail_equ += global_parity.count(1)
        loss_amount += global_parity.count(0)
        global_group += global_parity
        # Available equations are no less than loss blocks means repairable.
        if avail_equ < loss_amount:
            return False
        else:
            return True

    # If state needs both local repair and global repair, which one first?
    # Maybe we need another function, return the state after repair?
    # Tentatively, we return a tuple (repair_cost, state_after_repair)in
    #  this function, this is different with MDS codes.
    def stateRepairCost(self, state):
        if not self.is_repairable(state):
            return -1

        local_groups = self._extract_local(state)
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

        if failure_groups == 1 and state[-self.m1:].count(0) == 0:
            return self.optimalRepairCost() + local_failures - 1
        else:
            return self.normalRepairCost() + state.count(0) - 1


if __name__ == "__main__":
    lrc = LRC({'k': 6, 'l': 2, 'm0': 1, 'm1': 2})
    print lrc.is_MDS
    print lrc.deviceStorageCost()
    print lrc.systemStorageCost()
    print lrc.optimalRepairCost()
    print lrc.normalRepairCost()

    state = [1, 1, 1, 0, 0, 0, 0, 1, 1, 1]
    print lrc.is_repairable(state)
    print lrc.stateRepairCost(state)
    print lrc.stateParallRepairCost(state)
