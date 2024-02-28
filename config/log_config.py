import logging


def setup_logging():
    log_format = "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO,
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')
