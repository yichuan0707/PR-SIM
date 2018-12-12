from simulator.redundancy.MinStorageRegen import MSR


class MBR(MSR):

    # MBR stores more data on each node with sub-blocks.
    def deviceStorageCost(self):
        return float(2 * self.d)/float(2 * self.d - self.k + 1)

    def optimalRepairCost(self):
        return float(2 * self.d)/float(2 * self.d - self.k + 1)


if __name__ == "__main__":
    mbr = MBR({'k': 6, 'm': 3, 'd': 6})
    print "device stor:", mbr.deviceStorageCost()
    print "system stor:", mbr.systemStorageCost()
    print "optimal repair:", mbr.optimalRepairCost()
    print "normal repair:", mbr.normalRepairCost()
