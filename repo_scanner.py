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
    _ext_pattern = re.compile(r'\.([0-9a-zA-Z]*)$')

    @staticmethod
    def scan_repo(repo_id):
        # TODO:
        #  - organize exception handling for database operations
        #  - when DbManager error, raise an error to the messenger class

        print('Scanning directory \'{}\\{}\''.format(RepoScanner._repos_download_path, repo_id))
        try:
            repo_lang_rs = DbManager.insert_repository_languages(repo_id)
            # Dictionary that maps language id to corresponding
            # repository_language id of current repo
            repo_lang_ids = dict(repo_lang_rs)
        except Exception as e:
            print("DbManager exception:", e)
        else:
            for dirpath, dirs, files in os.walk(os.path.join(RepoScanner._repos_download_path, repo_id)):
                # For every file found in the directory
                for filename in files:
                    # Extract extension from the name
                    ext_search = RepoScanner._ext_pattern.search(filename)
                    extension = ext_search.group(1) if ext_search else None
                    # Get language id by file extension
                    lang_id = ExtLangMapper.get_id(extension)
                    if lang_id:
                        file_path = os.path.join(dirpath, filename)
                        # print('Inserting source file \'{}\' to database'.format(file_path))
                        try:
                            DbManager.insert_repository_language_file(repo_lang_ids[lang_id], file_path)
                        except Exception as e:
                            print("DbManager exception:", e)

