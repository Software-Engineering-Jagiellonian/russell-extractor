# Configure logging
import logging
logging.basicConfig(
    handlers=[logging.StreamHandler()],
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%H:%M:%S')


from messenger import Messenger
from db_manager import DbManager
from config import RABBITMQ_HOST, RABBITMQ_PORT

# Add package directory to system path (not necessary)
# from os.path import dirname, abspath, join
# import sys
# sys.path.append(dirname(__file__))


if __name__ == '__main__':
    logging.info("Starting frege-extractor app")
    DbManager.init_logger()
    Messenger().app(RABBITMQ_HOST, RABBITMQ_PORT)

