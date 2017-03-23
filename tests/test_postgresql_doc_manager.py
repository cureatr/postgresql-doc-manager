# Copyright 2016 Cureatr, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the PostgreSQL DocManager."""
import bson
from mongo_connector.compat import u
from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.postgresql_doc_manager import DocManager
from mongo_connector import constants
from mongo_connector.test_utils import TESTARGS

import unittest
from tests.test_postgresql import PostgreSQLTestCase


class TestPostgrSQLDocManager(PostgreSQLTestCase):
    """Unit tests for the PostgreSQL DocManager."""

    def setUp(self):
        super(TestPostgrSQLDocManager, self).setUp()
        self.docman = DocManager(self.postgresql.url())

    def _create_table(self):
        with self.docman._transaction() as cursor:
            self.docman._create_table(cursor, TESTARGS[0])

    def test_update(self):
        """Test the update method."""
        self._create_table()
        doc_id = bson.ObjectId()
        doc = {"_id": doc_id, "a": 1, "b": 2}
        self.docman.upsert(doc, *TESTARGS)
        # $set only
        update_spec = {"$set": {"a": 1, "b": 2}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": doc_id, "a": 1, "b": 2})
        # $unset only
        update_spec = {"$unset": {"a": True}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": doc_id, "b": 2})
        # mixed $set/$unset
        update_spec = {"$unset": {"b": True}, "$set": {"c": 3}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": doc_id, "c": 3})

    def test_upsert(self):
        """Test the upsert method."""
        self._create_table()
        doc_id = bson.ObjectId()
        docc = {'_id': doc_id, 'name': 'John'}
        self.docman.upsert(docc, *TESTARGS)
        res = self._query()
        for doc in res:
            doc = doc['document']
            self.assertEqual(doc['_id'], doc_id)
            self.assertEqual(doc['name'], 'John')

    def test_bulk_upsert(self):
        """Test the bulk_upsert method."""
        # This will create the table so the following will upsert
        self.docman.bulk_upsert([], *TESTARGS)

        docs = ({"_id": i} for i in range(1000))
        self.docman.bulk_upsert(docs, *TESTARGS)
        self.docman.commit()
        returned_ids = sorted(int(doc["document"]["_id"]) for doc in self._query())
        self.assertEqual(self._count(), 1000)
        self.assertEqual(len(returned_ids), 1000)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, i)

        docs = ({"_id": i, "weight": 2 * i} for i in range(1000))
        self.docman.bulk_upsert(docs, *TESTARGS)

        returned_ids = sorted(doc["document"]["weight"] for doc in self._query())
        self.assertEqual(len(returned_ids), 1000)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, 2 * i)

    def test_bulk_upsert_insert(self):
        """Test the bulk_upsert method on empty table."""
        count = int(constants.DEFAULT_MAX_BULK * 3.5)
        docs = ({"_id": i} for i in range(count))
        self.docman.bulk_upsert(docs, *TESTARGS)
        self.docman.commit()
        returned_ids = sorted(int(doc["document"]["_id"]) for doc in self._query())
        self.assertEqual(self._count(), count)
        self.assertEqual(len(returned_ids), count)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, i)

    def test_remove(self):
        """Test the remove method."""
        self._create_table()
        docc = {'_id': '1', 'name': 'John'}
        self.docman.upsert(docc, *TESTARGS)
        res = self._query()
        res = [x["document"] for x in res]
        self.assertEqual([docc], res)

        self.docman.remove(docc['_id'], *TESTARGS)
        res = self._query()
        self.assertEqual(len(res), 0)

    def test_search(self):
        """Test the search method.

        Make sure we can retrieve documents last modified within a time range.
        """
        self._create_table()
        doc_id1 = bson.ObjectId()
        docc = {'_id': doc_id1, 'name': 'John'}
        self.docman.upsert(docc, 'test.test', 5767301236327972865)
        doc_id2 = '2'
        docc2 = {'_id': doc_id2, 'name': 'John Paul'}
        self.docman.upsert(docc2, 'test.test', 5767301236327972866)
        docc3 = {'_id': bson.ObjectId(), 'name': 'Paul'}
        self.docman.upsert(docc3, 'test.test', 5767301236327972870)
        search = list(self.docman.search(5767301236327972865, 5767301236327972866))
        self.assertEqual(len(search), 2)
        result_ids = {result.get("_id") for result in search}
        self.assertEqual({u(doc_id1), doc_id2}, result_ids)

    def test_get_last_doc(self):
        """Test the get_last_doc method.

        Make sure we can retrieve the document most recently modified from PostgreSQL.
        """
        self._create_table()
        base = self.docman.get_last_doc()
        ts = base.get("_ts", 0) if base else 0
        docc = {'_id': '4', 'name': 'Hare'}
        self.docman.upsert(docc, 'test.test', ts + 3)
        docc = {'_id': '5', 'name': 'Tortoise'}
        self.docman.upsert(docc, 'test.test', ts + 2)
        docc = {'_id': '6', 'name': 'Mr T.'}
        self.docman.upsert(docc, 'test.test', ts + 1)

        self.assertEqual(self._count(), 3)
        doc = self.docman.get_last_doc()
        self.assertEqual(doc['_id'], '4')

        docc = {'_id': '6', 'name': 'HareTwin'}
        self.docman.upsert(docc, 'test.test', ts + 4)
        doc = self.docman.get_last_doc()
        self.assertEqual(doc['_id'], '6')
        self.assertEqual(self._count(), 3)

    def test_commands(self):
        self._create_table()
        cmd_args = ('test.$cmd', 1)
        self.docman.command_helper = CommandHelper()

        self.docman.handle_command({'create': 'test2'}, *cmd_args)
        self.assertEqual(0, self._count("test.test2"))

        docs = [
            {"_id": 0, "name": "ted"},
            {"_id": 1, "name": "marsha"},
            {"_id": 2, "name": "nikolas"}
        ]
        self.docman.upsert(docs[0], 'test.test2', 1)
        self.docman.upsert(docs[1], 'test.test2', 1)
        self.docman.upsert(docs[2], 'test.test2', 1)
        res = [x['document'] for x in self._query()]
        for d in docs:
            self.assertTrue(d in res)

        self.docman.handle_command({'drop': 'test2'}, *cmd_args)
        self.assertEqual(0, self._count())

        def check_table(table):
            return self._query("""SELECT tablename FROM pg_tables WHERE tablename = %s""", (table,))

        self.docman.handle_command({'create': 'test2'}, *cmd_args)
        self.assertTrue(check_table("test.test2"))
        self.docman.handle_command({'create': 'test3'}, *cmd_args)
        self.assertTrue(check_table("test.test3"))
        self.docman.handle_command({'dropDatabase': 1}, *cmd_args)
        self.assertFalse(check_table("test.test2"))
        self.assertFalse(check_table("test.test3"))


if __name__ == '__main__':
    unittest.main()
