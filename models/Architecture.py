import os
from ConfigParser import ConfigParser

from simulator.utils.Tree import Tree
from simulator.Configuration import Configuration, CONF_PATH


# Maybe class Layer can inherit class Tree?
class Layer(object):

    def __init__(self, layer_id):
        layer_path = CONF_PATH + os.sep + "layer_" + str(layer_id) + ".ini"
        self.layer_id = layer_id
        self.conf = ConfigParser()
        self.conf.read(layer_path)
        self.dcs = dict(self.conf.items("DataCenter"))
        self.racks = dict(self.conf.items("Rack"))
        self.machines = dict(self.conf.items("Machine"))
        self.disks = dict(self.conf.items("Disk"))


def returnLayers():
    layers = []
    conf = Configuration()
    for i in xrange(1, conf.tier_num+1):
        layers.append(Layer(i))
    return layers


def returnLayerArch(layer):
    conf = Configuration()
    dcs = conf.datacenters
    racks = conf.racks
    machines = conf.machines_per_rack
    disks = conf.disks_per_machine

    layer_tree = Tree("SYS")
    for dc_id in xrange(dcs):
        dc_node = layer_tree.addChild("DC" + str(dc_id))
        for rack_id in xrange(racks):
            rack_node = dc_node.addChild("R" + str(rack_id))
            for machine_id in xrange(machines):
                machine_node = rack_node.addChild("M" + str(machine_id))

                # Haven't consider the medium is SSD.
                for disk_id in xrange(disks):
                    machine_node.addChild("H" + str(disk_id))

    return layer_tree


if __name__ == "__main__":
    layer1 = Layer(1)
    layer1_arch = returnLayerArch(layer1)
    print layer1_arch.getChildrenKeys
    print layer1_arch.returnLeavesFullName(layer1_arch)
