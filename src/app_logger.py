import logging
import sys
import src.constants as cst

class AppLogger:
    @staticmethod
    def configure_logger():
        logging.basicConfig(
            format = cst.LOG_FORMAT,
            handlers = [
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(cst.LOG_FILE_PATH)
            ]
        )

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)