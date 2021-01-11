import logging
logging.basicConfig(
    handlers=[logging.StreamHandler()],
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%H:%M:%S')

from os.path import dirname, abspath, join
import sys

from messenger import Messenger
from ext_lang_mapper import ExtLangMapper
# from logger import logger
# Add package directory to system path
# sys.path.append(dirname(__file__))


if __name__ == '__main__':
    print("running main.py")
    logging.info("start")
    ExtLangMapper.init()
    Messenger.app('127.0.0.1', '5672')

