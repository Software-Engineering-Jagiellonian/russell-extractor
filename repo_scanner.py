import os
import re
from extractor.db_manager import DbManager
from extractor.ext_lang_mapper import ExtLangMapper


class RepoScanner:
    """Scanner of repository folder located in the file system.
    Finds source files by extension."""

    # Path for the downloaded repos
    _repos_download_path = 'sample-repo'
    # File extension pattern
    __ext_pattern = re.compile(r'\.([0-9a-zA-Z]*)$')

    @staticmethod
    def scan_repo(repo_id):
        """Performs scanning of repository folder by its id
        :returns set of ids of languages found in repo"""
        # TODO:
        #  [x] organize exception handling for database operations
        #  [x] when DbManager error, raise an error to the messenger class
        #  [x] update repository_language.present column
        #  [ ] raise exception if repo folder is not found in the filesystem

        print('Scanning directory \'{}\\{}\''.format(RepoScanner._repos_download_path, repo_id))
        try:
            # Dictionary that maps language id to repository_language entry id
            repo_lang_ids = dict(DbManager.insert_repository_languages(repo_id))
            # Set of ids of the present languages in repo
            present_langs = set()
            for dirpath, dirs, files in os.walk(os.path.join(RepoScanner._repos_download_path, repo_id)):
                # For every file found in the directory
                for filename in files:
                    # Extract extension from the name
                    extension = RepoScanner._get_extension(filename)
                    # Get language id by file extension
                    lang_id = ExtLangMapper.get_id(extension)
                    if lang_id:
                        file_path = os.path.join(dirpath, filename)
                        print("Found a source file {}".format(file_path))
                        # If the language wasn't present in this repo yet
                        if lang_id not in present_langs:
                            # Set this language as present in DB
                            DbManager.update_repository_language_present(repo_id, lang_id)
                            # Update the dictionary of the present languages
                            present_langs.add(lang_id)
                        DbManager.insert_repository_language_file(repo_lang_ids[lang_id], file_path)
            return present_langs
        except Exception as e:
            # print("Exception while scanning repository:", e)
            raise e

    @staticmethod
    def _get_extension(filename):
        ext_search = RepoScanner.__ext_pattern.search(filename)
        return ext_search.group(1) if ext_search else None
