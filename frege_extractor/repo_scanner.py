import os
import re
import logging
from db_manager import DbManager
from ext_lang_mapper import ExtLangMapper
from config import REPOSITORIES_DIRECTORY


class RepoScanner:
    """Scanner of repository folder located in the file system.
    Finds source files by extension."""

    # File extension pattern
    __ext_pattern = re.compile(r'\.([0-9a-zA-Z]*)$')

    # Dictionary that maps languages' ids to their names
    __language_id_name = dict(DbManager.select_languages())

    @staticmethod
    def run_scanner(repo_id):
        """Performs scanning of repository folder by its id
        :returns set of names of languages found in repo"""

        logging.info("Scanning directory {}".format(os.path.join(REPOSITORIES_DIRECTORY, repo_id)))
        # Raise error if repo_id is not found among folders
        if repo_id not in os.listdir(REPOSITORIES_DIRECTORY):
            raise Exception('\'{}\' was not found in the download directory of repositories'.format(repo_id))

        files_langs, present_langs = RepoScanner.get_repo_files_langs(repo_id)
        if not files_langs:
            logging.warning("No known source files found in the repo")

        RepoScanner._db_insert_repo_languages_files(repo_id, present_langs, files_langs)

        # Make list of names of languages from their ids
        return [RepoScanner.__language_id_name[lang_id] for lang_id in present_langs]

    @staticmethod
    def get_repo_files_langs(repo_id):
        """Extracts source files and the present languages from the repo folder
        :returns tuple of found files' paths and present languages' IDs"""
        # Set of ids of the present languages in repo
        present_lang_ids = set()
        files_langs = []
        for dirpath, dirs, files in os.walk(os.path.join(REPOSITORIES_DIRECTORY, repo_id)):
            # For every file found in the directory
            for filename in files:
                # Extract extension from the name
                extension = RepoScanner.get_file_extension(filename)
                # Get language id by file extension
                lang_id = ExtLangMapper.get_language_id(extension)
                if lang_id:
                    file_path = os.path.join(dirpath, filename)
                    logging.info("Found a source file {}".format(file_path))
                    files_langs.append((file_path, lang_id))
                    present_lang_ids.add(lang_id)
        return files_langs, list(present_lang_ids)

    @staticmethod
    def _db_insert_repo_languages_files(repo_id, present_langs, files_langs):
        """Inserts repository_language and repository_language_file entries in the database"""
        # Dictionary that maps language id to repository_language entry id
        repo_lang_ids = dict(DbManager.insert_repository_languages(repo_id, present_langs))

        # Make list of files and corresponding repository_language ids
        repo_lang_files = [(repo_lang_ids[lang], file) for (file, lang) in files_langs]
        DbManager.insert_repository_language_files(repo_lang_files)

    @staticmethod
    def get_file_extension(filename):
        """:returns extension from a file name string. If no extension, returns None"""
        ext_search = RepoScanner.__ext_pattern.search(filename)
        return ext_search.group(1) if ext_search else None
