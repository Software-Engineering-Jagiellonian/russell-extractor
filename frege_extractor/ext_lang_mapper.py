import logging
import sys
from db_manager import DbManager


class ExtLangMapper:
    """Class that maps source file extension to the
    id of the language from the database"""

    def __init__(self):
        # Dictionary that defines valid source file extensions for each language name
        # Put here extensions that should recognized by RepoScanner
        self._lang_extensions = {
            'C': ['c', 'h'],
            'C++': ['cpp', 'hpp', 'cxx', 'hxx', 'C', 'H'],
            'C#': ['cs'],
            'CSS': ['css'],
            'Java': ['java'],
            'JS': ['js'],
            'PHP': ['php'],
            'Python': ['py'],
            'Ruby': ['rb'],
        }
        self._extension_lang_id = dict()
        self._extension_lang_name = dict()
        logging.info("Initializing extension-language-id mapper")
        try:
            lang_name_id = dict((name, id) for id, name in DbManager.select_languages())
            # Make extension_lang_id a dictionary that maps extensions from
            # __extension_lang to language ids from language result set
            for name, extensions in self._lang_extensions.items():
                for ext in extensions:
                    self._extension_lang_id[ext] = lang_name_id[name]
                    self._extension_lang_name[ext] = name
        except KeyError as ke:
            logging.error(f"Exception while initializing extension-language mapper. "
                          f"Did not found {ke} language in the database, which "
                          f"is defined in the mapper.")
            sys.exit(1)

    def get_language_id(self, extension):
        return self._extension_lang_id.get(extension)

    def get_language_name(self, extension):
        return self._extension_lang_name.get(extension)
