from simulator.redundancy.base import Base


class RS(Base):

    def stateRepairCost(self, state):
        if not self.is_repairable(state):
            return -1
        return self.k

    # Suppose there is a (a < m) failures in one stripe. Parallel Repair
    # starts on node hold one failure, downloads k blocks, then
    # calculates, at last uploads a-1 failures to destinations.
    def stateParallRepairCost(self, state):
        if not self.is_repairable(state):
            return - 1

        return state.count(0) + self.k - 1


if __name__ == "__main__":
    rs = RS({'k': 6, 'm': 3})
    print rs.is_MDS
    print rs.deviceStorageCost()
    print rs.systemStorageCost()
    print rs.dataSpreading()
    print rs.normalRepairCost()
    print rs.is_repairable([1, 1, 1, 0, 1, 0, 1, 1, 1])
    print rs.stateRepairCost([1, 1, 1, 1, 1, 0, 0, 1, 1])
    print rs.stateParallRepairCost([1, 0, 0, 1, 0, 0, 1, 1, 1])
