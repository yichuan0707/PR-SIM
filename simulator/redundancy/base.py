

class Base(object):

    def __init__(self, params):
        self.k = params['k']
        self.m = params['m']
        self.n = self.k + self.m
        self._check()

    def _check(self):
        if self.k < 0 or self.m < 0:
            raise Exception("k and m must be positive integer!")

    @property
    def is_MDS(self):
        return True

    # number of blocks on device.
    def deviceStorageCost(self):
        return 1

    # total block number for one stripe.
    def systemStorageCost(self):
        return (self.n * self.deviceStorageCost())

    # data spreading array.
    def dataSpreading(self):
        dsc = self.deviceStorageCost()
        return [dsc for item in xrange(self.n)]

    def normalRepairCost(self):
        return self.k

    # Check 'state' can be recovered or not. If can be recovered, return the
    # corresponding repair cost, or return False.
    # state = [1, 0, 1, 0, ..., 1], 1 for available, 0 for unavailable.
    def is_repairable(self, state):
        if len(state) != self.n:
            raise Exception("State Length Error!")

        avails = state.count(1)
        if avails == self.n:
            raise Exception("No Failures!")

        if avails >= self.k:
            return True
        return False

    # Repair failures one by one.
    def stateRepairCost(self, state):
        pass

    # Repair all failures simultaneously.
    def stateParallRepairCost(self, state):
        pass


class BaseWithoutParititioning(object):
    """
    Base data redundancy scheme Class for unparititioning system, no blocks
    in it, someone calls 'online encoding'.
    """
    pass


if __name__ == "__main__":
    pass
