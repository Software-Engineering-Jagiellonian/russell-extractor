from os.path import dirname, abspath, join
import sys
# Find code directory relative to our directory
# THIS_DIR = dirname(__file__)
# print('THIS_DIR:', THIS_DIR)
# CODE_DIR = abspath(join(THIS_DIR, '..', 'frege_extractor'))
# print('CODE_DIR:', CODE_DIR)
sys.path.append(dirname(__file__))
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
from ext_lang_mapper import ExtLangMapper
ExtLangMapper.init()
