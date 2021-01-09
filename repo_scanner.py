import os
import re
import logging
from extractor.db_manager import DbManager
from extractor.ext_lang_mapper import ExtLangMapper


class RepoScanner:
    """Scanner of repository folder located in the file system.
    Finds source files by extension."""

    # Path for the downloaded repos
    # _repos_download_path = 'sample-repo'
    _repos_download_path = os.environ.get('REPOSITORIES_DIRECTORY', 'sample-repo')

    # File extension pattern
    __ext_pattern = re.compile(r'\.([0-9a-zA-Z]*)$')

    # Dictionary that maps languages' ids to their names
    __language_id_name = dict(DbManager.select_languages())

    @staticmethod
    def scan_repo(repo_id):
        """Performs scanning of repository folder by its id
        :returns set of names of languages found in repo"""
        # TODO:
        #  [x] organize exception handling for database operations
        #  [x] when DbManager error, raise an error to the messenger class
        #  [x] update repository_language.present column
        #  [x] raise exception if repo folder is not found in the filesystem

        logging.info("Scanning directory {}".format(os.path.join(RepoScanner._repos_download_path, repo_id)))
        try:
            # Raise error if repo_id is not found among folders
            if repo_id not in os.listdir(RepoScanner._repos_download_path):
                raise Exception('\'{}\' was not found in the download directory of repositories'.format(repo_id))
            # Dictionary that maps language id to repository_language entry id
            repo_lang_ids = dict(DbManager.insert_repository_languages(repo_id))
            # Set of ids of the present languages in repo
            present_langs = set()
            for dirpath, dirs, files in os.walk(os.path.join(RepoScanner._repos_download_path, repo_id)):
                # For every file found in the directory
                for filename in files:
                    # Extract extension from the name
                    extension = RepoScanner._get_file_extension(filename)
                    # Get language id by file extension
                    lang_id = ExtLangMapper.get_language_id(extension)
                    if lang_id:
                        file_path = os.path.join(dirpath, filename)
                        logging.info("Found a source file {}".format(file_path))
                        # If the language wasn't present in this repo yet
                        if lang_id not in present_langs:
                            # Set this language as present in DB
                            DbManager.update_repository_language_present(repo_id, lang_id)
                            # Update the dictionary of the present languages
                            present_langs.add(lang_id)
                        DbManager.insert_repository_language_file(repo_lang_ids[lang_id], file_path)
            # Make list of names of languages from set of their ids
            return [RepoScanner.__language_id_name[lang__id] for lang__id in present_langs]
        except Exception as e:
            # logging.error("Error while scanning repository:", e)
            raise e

    @staticmethod
    def _get_file_extension(filename):
        ext_search = RepoScanner.__ext_pattern.search(filename)
        return ext_search.group(1) if ext_search else None
