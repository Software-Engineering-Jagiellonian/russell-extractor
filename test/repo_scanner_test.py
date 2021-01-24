import unittest
from extractor.frege_extractor.repo_scanner import RepoScanner


class RepoScannerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.repo_scanner = RepoScanner('repo_test_dir')

    def test_get_files_langs_sample(self):
        result_files, result_langs = self.repo_scanner.get_repo_files_langs('fibonacci-lcs')
        target_files = {
            ('fibonacci-lcs\\fibonacci\\fibonacci_rb.rb', 9),
            ('fibonacci-lcs\\fibonacci\\fibonacci_py.py', 8),
            ('fibonacci-lcs\\fibonacci\\fibonacci_js.js', 6),
            ('fibonacci-lcs\\fibonacci\\fibonacci_java.java', 5),
            ('fibonacci-lcs\\fibonacci\\fibonacci_cs.cs', 3),
            ('fibonacci-lcs\\fibonacci\\fibonacci_cpp.cpp', 2),
            ('fibonacci-lcs\\lcs\\lcs_py.py', 8),
            ('fibonacci-lcs\\lcs\\lcs_js.js', 6),
            ('fibonacci-lcs\\lcs\\lcs_java.java', 5),
            ('fibonacci-lcs\\lcs\\lcs_cs.cs', 3),
            ('fibonacci-lcs\\lcs\\lcs_cpp.cpp', 2),
        }
        target_langs = {9, 8, 6, 5, 3, 2}
        # Assert found files
        self.assertSetEqual(set(result_files), target_files)
        # Assert found languages
        self.assertSetEqual(set(result_langs), target_langs)

    def test_get_files_langs_empty(self):
        result_files, result_langs = self.repo_scanner.get_repo_files_langs('empty-repo')
        self.assertListEqual(result_files, [])
        self.assertListEqual(result_langs, [])


if __name__ == '__main__':
    unittest.main()
