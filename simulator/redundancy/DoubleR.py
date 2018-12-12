from simulator.redundancy.base import Base


class DR(Base):

    def __init__(self, params):
        super(DR, self).__init__(params)
        self.r = params['r']

    def _check(self):
        super(DR, self)._check()
        if self.r < 0:
            raise Exception("r must be positive integer!")