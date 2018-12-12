

class Result(object):
    unavailable_count = 0
    undurable_count = 0

    def toString(self):
        return "unavailable=" + str(Result.unavailable_count) + \
            "  undurable=" + str(Result.undurable_count)
