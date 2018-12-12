
class Tree(object):
    """
    Tree for hardware architecture.
    1: System(SYS)
    2: DataCenters(DC0, DC1, ...)
    3: Racks(R0, R1, R2, R3, ...)
    4: Machines(M0, M1, M2, M3, ...)
    5: Storage Drives(H0, H1, H2, ...(for HDDs);
                      S0, S1, S2, ...(for SSDs))
    """
    def __init__(self, root, parent=None):
        self.key = root
        self.parent = parent
        self.children = []

    @property
    def getChildrenKeys(self):
        childern_keys = [item.key for item in self.children]
        childern_keys.sort()
        return childern_keys

    @property
    def getParentKey(self):
        return self.parent.key

    def addChild(self, new_key):
        new_node = Tree(new_key, self)
        if new_node in self.children:
            raise Exception("key already exists!")

        insert_id = 0
        for i, child in enumerate(self.children):
            if new_key < child.key:
                insert_id = i
                break
        self.children.insert(insert_id, new_node)

        return new_node

    # Return child index in parent's children list, not the child node itself.
    def getChild(self, key):
        childern_keys = self.getChildrenKeys
        if key not in childern_keys:
            return -1
            # raise Exception("key %s doesn't have child %s" % (self.key, key))

        return childern_keys.index(key)

    def deleteChild(self, delete_key):
        if delete_key not in self.getChildrenKeys:
            raise Exception("delete key not exists!")

        for child in self.children:
            if child.key == delete_key:
                self.children.remove(child)

    def getDepth(self):
        if self.key == "SYS":
            depth = 1
        elif self.key.startswith("DC"):
            depth = 2
        elif self.key.startswith("R"):
            depth = 3
        elif self.key.startswith("M"):
            depth = 4
        elif self.key.startswith("H") or (self.key.startswith("S") and
                                          self.key != "SYS"):
            depth = 5
        else:
            depth = -1

        return depth

    @classmethod
    def returnLeavesFullName(cls, root):
        full_name = []

        if root.parent is not None:
            raise Exception("Given parameter is not root node!")

        # Get all leaves' name by depth-first traversal.
        def _get_child_fullname(nodes, base_name):
            for node in nodes:
                if node.children == []:
                    full_name.append(base_name + '-' + node.key)
                else:
                    _get_child_fullname(node.children, base_name + '-' +
                                        node.key)

        if root.children == []:
            full_name.append(root.key)
        else:
            _get_child_fullname(root.children, root.key)

        return full_name


"""
def testTree():
    sys = Tree("SYS")
    dc0 = sys.addChild("DC0")
    dc1 = sys.addChild("DC1")
    r0 = dc0.addChild("R0")
    r1 = dc0.addChild("R1")
    r2 = dc0.addChild("R2")
    r0m0 = r0.addChild("M0")
    r0m1 = r0.addChild("M1")
    r1m0 = r1.addChild("M0")
    r1m1 = r1.addChild("M1")
    r0m0h0 = r0m0.addChild("H0")
    r0m0h1 = r0m0.addChild("H1")
    r0m0h2 = r0m0.addChild("H2")
    r0m0s0 = r0m0.addChild("S0")
    r1m0h0 = r1m0.addChild("H0")

    full_name = Tree.returnLeavesFullName(sys)
    print full_name
    for item in r0m0.children:
        print item.key
    print r0m0h0.parent.key
    print r0m0.getChild("H0")
    print r0m0h0.getDepth()
"""

if __name__ == "__main__":
    # testTree()
    pass
