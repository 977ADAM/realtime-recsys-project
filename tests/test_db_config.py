import os
import unittest
from unittest.mock import patch

from app.db import _database_url


class DbConfigTests(unittest.TestCase):
    def test_database_url_raises_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                _database_url()

    def test_database_url_returns_trimmed_value(self):
        with patch.dict(os.environ, {"DATABASE_URL": "  postgresql://u:p@localhost:5432/recsys  "}, clear=True):
            self.assertEqual(_database_url(), "postgresql://u:p@localhost:5432/recsys")


if __name__ == "__main__":
    unittest.main()
