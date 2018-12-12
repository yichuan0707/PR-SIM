import logging
import logging.config

logging.config.fileConfig(r"/root/PR-Sim/conf/log.conf")
global info_logger
info_logger = logging.getLogger("infoLogger")

global error_logger
error_logger = logging.getLogger("errorLogger")


if __name__ == "__main__":
    # logging.config.fileConfig("D:\\nut cloud\\Codes\\PR-Sim\\conf\\log.conf")
    # info_logger = logging.getLogger('infoLogger')
    # info_logger.info("test")
    # error_logger = logging.getLogger('errorLogger')
    # error_logger.error("error")
    pass
