import logging
from extractor.db_manager import DbManager


class ExtLangMapper:
    """Class that maps source file extension to the
    id of the language from the database"""

    __lang_extensions = {
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

    _extension_lang_id = dict()
    _extension_lang_name = dict()

    @staticmethod
    def get_language_id(extension):
        return ExtLangMapper._extension_lang_id.get(extension)

    @staticmethod
    def get_language_name(extension):
        return ExtLangMapper._extension_lang_name.get(extension)

    @staticmethod
    def init():
        logging.info("Initializing extension-language-id mapper")
        lang_name_id = dict((name, id) for id, name in DbManager.select_languages())
        # Make extension_lang_id a dictionary that maps extensions from
        # __extension_lang to language ids from language result set
        for name, extensions in ExtLangMapper.__lang_extensions.items():
            for ext in extensions:
                ExtLangMapper._extension_lang_id[ext] = lang_name_id[name]
                ExtLangMapper._extension_lang_name[ext] = name
