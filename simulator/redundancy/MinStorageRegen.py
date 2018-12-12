from simulator.redundancy.base import Base


class MSR(Base):

    def __init__(self, params):
        self.d = params['d']
        super(MSR, self).__init__(params)

    def _check(self):
        super(MSR, self)._check()
        if self.d < self.k or self.d >= self.n:
            raise Exception("d must be in [k, n).")

    # When available blocks in state are no less than 'd', MSR can proceed
    # optimal repair.
    # Return value in blocks(Configuration.block_size).
    def optimalRepairCost(self):
        return float(self.d)/float(self.d - self.k + 1)

    def stateRepairCost(self, state):
        if not self.is_repairable(state):
            return -1

        avails = state.count(1)
        if avails >= self.d:
            return self.optimalRepairCost()
        else:
            return self.k

    # MSR can not recover multi failures through regenerating even available
    # blocks larger than 'd'.
    def stateParallRepairCost(self, state):
        if not self.is_repairable(state):
            return -1

        avails = state.count(1)
        if avails == self.n - 1:
            return self.optimalRepairCost()
        else:
            return state.count(0) + self.k - 1


if __name__ == "__main__":
    msr = MSR({'k': 10, 'm': 4, 'd': 12})
    print msr.systemStorageCost()
    print msr.stateParallRepairCost([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1])
