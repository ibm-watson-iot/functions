from iotfunctions.db import Database
import unittest
from unittest.mock import patch
import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')))


class TestDB_Methods(unittest.TestCase):
    @patch.object(Database, 'get_table')
    @patch.object(Database, 'cache_entity_types')
    def test_find_dimension_table_missing(self, mock_cache_entity_types, mock_get_table):
        mockDatabase = Database(tenant_id="mytenant", db2_installed=False)
        # pretend that we can't find the table
        mock_get_table.side_effect = KeyError('no table called that')
        [dimension, dim] = mockDatabase.find_dimension_table(
            'dimension', 'schema')
        self.assertEquals(dimension, None)
        self.assertEquals(dim, None)
    @patch.object(Database, 'get_table')
    @patch.object(Database, 'cache_entity_types')
    def test_find_dimension_table_found(self, mock_cache_entity_types, mock_get_table):
        mockDatabase = Database(tenant_id="mytenant", db2_installed=False)
        # Verify if we do find the table it works
        mock_get_table.return_value = 'mydim'
        [dimension, dim] = mockDatabase.find_dimension_table(
            'dimension', 'schema')
        self.assertEquals(dimension, 'dimension')
        self.assertEquals(dim, 'mydim')


if __name__ == '__main__':
    unittest.main()
