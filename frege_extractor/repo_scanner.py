import os
import re
import logging
from db_manager import DbManager
from ext_lang_mapper import ExtLangMapper
from config import REPOSITORIES_DIRECTORY


class RepoScanner:
    """Scanner of repository folder located in the file system.
    Finds source files by extension."""

    def __init__(self):
        # File extension pattern
        self.__ext_pattern = re.compile(r'\.([0-9a-zA-Z]*)$')

        # Dictionary that maps languages' ids to their names
        self.__language_id_name = dict(DbManager.select_languages())

    def run_scanner(self, repo_id):
        """Performs scanning of repository folder by its id
        :returns set of names of languages found in repo"""

        logging.info("Scanning directory {}".format(os.path.join(REPOSITORIES_DIRECTORY, repo_id)))
        # Raise error if repo_id is not found among folders
        if repo_id not in os.listdir(REPOSITORIES_DIRECTORY):
            raise Exception('\'{}\' was not found in the download directory of repositories'.format(repo_id))

        # lang_ids_for_repo = self._db_get_languages_for_repo(repo_id)
        files_langs, present_langs = self.get_repo_files_langs(repo_id)

        if not files_langs:
            logging.warning("No known source files found in the repo")

        self._db_insert_repo_languages_files(repo_id, present_langs, files_langs)

        # Make list of names of languages from their ids
        return [self.__language_id_name[lang_id] for lang_id in present_langs]

    def get_repo_files_langs(self, repo_id):
        """Extracts source files and the present languages from the repo folder
        :returns tuple of found files' paths and present languages' IDs"""
        # Set of ids of the present languages in repo
        present_lang_ids = set()
        files_langs = []
        os.chdir(REPOSITORIES_DIRECTORY)
        for dirpath, dirs, files in os.walk(repo_id):
            # For every file found in the directory
            for filename in files:
                # Extract extension from the name
                extension = self.get_file_extension(filename)
                # Get language id by file extension
                lang_id = ExtLangMapper.get_language_id(extension)
                if lang_id:
                    file_path = os.path.join(dirpath, filename)
                    logging.info("Found a source file {}".format(file_path))
                    files_langs.append((file_path, lang_id))
                    present_lang_ids.add(lang_id)
        os.chdir('..')
        return files_langs, list(present_lang_ids)

    def _db_get_languages_for_repo(self, repo_id):
        """:returns list of language IDs given for repository ID in
        repository_language table. If there are none, returns all language IDs"""
        repo_langs = DbManager.select_repository_languages(repo_id)
        if repo_langs:
            return [rl[0] for rl in repo_langs]
        else:
            return list(self.__language_id_name.keys())

    def _db_insert_repo_languages_files(self, repo_id, present_langs, files_langs):
        """Inserts repository_language entries in the database if there are none.
        Updates present languages according to found languages ids (present_langs).
        Inserts repository_language_file entries (files_langs)."""
        # repo_lang_ids - dictionary that maps language id to repository_language entry id for this repo
        # Select existing entries
        repo_lang_ids = dict(DbManager.select_repository_languages(repo_id))
        if not repo_lang_ids:
            # Insert new entries and return their ids
            repo_lang_ids = dict(DbManager.insert_repository_languages(repo_id))
        # Update present repository languages
        DbManager.update_present_repository_languages(repo_id, present_langs)
        try:
            # Make list of files and corresponding repository_language ids
            repo_lang_files = [(repo_lang_ids[lang], file) for (file, lang) in files_langs]
            # Insert all files to database
            DbManager.insert_repository_language_files(repo_lang_files)
        except KeyError as e:
            raise Exception(f"A repository_language entry for language {e} and repo \'{repo_id}\' is missing,"
                            f" but other entries for this repo were found. repository_language table should"
                            f" contain records for either all languages or none.")

    def get_file_extension(self, filename):
        """:returns extension from a file name string. If no extension, returns None"""
        ext_search = self.__ext_pattern.search(filename)
        return ext_search.group(1) if ext_search else None
