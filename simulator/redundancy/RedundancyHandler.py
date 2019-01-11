from simulator.redundancy.ReedSolomon import RS
from simulator.redundancy.LocalRepairCode import LRC
from simulator.redundancy.Xorbas import XORBAS
from simulator.redundancy.MinStorageRegen import MSR
from simulator.redundancy.MinBandRegen import MBR


def getRedunHandler(redun_name, params):
    if redun_name.upper() == "RS":
        handler = RS(params)
    elif redun_name.upper() == "LRC":
        handler = LRC(params)
    elif redun_name.upper() == "XORBAS":
        handler = XORBAS(params)
    elif redun_name.upper() == "MSR":
        handler = MSR(params)
    elif redun_name.upper() == "MBR":
        handler = MBR(params)
    else:
        raise Exception("Incorrect redundancy name!")
    return handler


if __name__ == "__main__":
    redun_name = "MBR"
    params = {"k":6, "d":7, "m":5, "m1":2}
    handler = getRedunHandler(redun_name, params)
    print handler.systemStorageCost()
