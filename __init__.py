import logging
from extractor.ext_lang_mapper import ExtLangMapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s: %(message)s', datefmt='%H:%M:%S')
ExtLangMapper.init()
