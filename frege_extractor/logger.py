import logging

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False
