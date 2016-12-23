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

"""Integration tests for mongo-connector + PostgrSQL 9.5."""
# Based on https://github.com/mongodb-labs/elastic2-doc-manager/tree/master/tests
import os
import time
import datetime

import bson
import testing.postgresql
import psycopg2
import psycopg2.extras

from mongo_connector.connector import Connector
from mongo_connector.doc_managers.postgresql_doc_manager import DocManager
from mongo_connector.doc_managers import postgresql_doc_manager
from mongo_connector.test_utils import (ReplicaSet,
                                        assert_soon,
                                        close_client)

from mongo_connector.util import retry_until_ok
import unittest

Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


def tearDownModule():
    Postgresql.clear_cache()


class PostgreSQLTestCase(unittest.TestCase):
    """Base class for all PostgreSQL TestCases."""

    def setUp(self):
        self.postgresql = Postgresql()
        self.postgresql_conn = psycopg2.connect(**self.postgresql.dsn())
        psycopg2.extras.register_default_jsonb(self.postgresql_conn, loads=bson.json_util.loads)

    def tearDown(self):
        self.postgresql.stop()

    def _query(self, query="SELECT * from mongodb_collections", query_args=None):
        with self.postgresql_conn, self.postgresql_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, query_args)
            return cursor.fetchall()

    def _count(self, namespace="mongodb_collections"):
        with self.postgresql_conn, self.postgresql_conn.cursor() as cursor:
            query = u"""SELECT COUNT(*) FROM "{}";""".format(namespace)
            cursor.execute(query)
            return cursor.fetchone()[0]


class TestPostgreSQL(PostgreSQLTestCase):
    """Integration tests for mongo-connector + PostgreSQL."""

    @classmethod
    def setUpClass(cls):
        """Start the cluster."""
        cls.repl_set = ReplicaSet().start()
        cls.conn = cls.repl_set.client(tz_aware=True)

    @classmethod
    def tearDownClass(cls):
        """Kill the cluster."""
        close_client(cls.conn)
        cls.repl_set.stop()

    def setUp(self):
        """Start a new Connector for each test."""
        super(TestPostgreSQL, self).setUp()
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        docman = DocManager(self.postgresql.url())
        self.connector = Connector(
            mongo_address=self.repl_set.uri,
            ns_set=['test.test', 'test.test1', 'test.test2'],
            doc_managers=(docman,),
        )

        self.conn.drop_database('test')

        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        assert_soon(lambda: self._count() == 0)

    def tearDown(self):
        """Stop the Connector thread."""
        super(TestPostgreSQL, self).tearDown()
        self.connector.join()
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass

    def test_insert(self):
        """Test insert operations."""
        _id = self.conn.test.test1.insert_one({'name1': 'jimmy', "date": datetime.datetime(2016, 3, 2, 11, 21), "mid": bson.ObjectId()}).inserted_id
        doc1 = self.conn.test.test1.find_one({"_id": _id})
        assert_soon(lambda: self._count() == 1)
        _id = self.conn.test.test2.insert_one({'name2': 'saul', "date": datetime.datetime(2014, 6, 4, 9, 33), "mid": bson.ObjectId()}).inserted_id
        doc2 = self.conn.test.test2.find_one({"_id": _id})
        assert_soon(lambda: self._count() == 2)

        # assert_soon(lambda: self._count() == 2)
        self.assertEqual(1, self._count("test.test1"))
        self.assertEqual(1, self._count("test.test2"))

        self.assertEqual([doc1], [item['document'] for item in self._query("""SELECT document FROM "test.test1";""")])
        self.assertEqual([doc2], [item['document'] for item in self._query("""SELECT document FROM "test.test2";""")])

    def test_insert_retry(self):
        """Test insert retry on PostgreSQL failure."""
        class TimeMock(object):
            def sleep(sec):
                time.sleep(1)

        try:
            # Monkey patch sleep
            postgresql_doc_manager.time = TimeMock()
            # Kill the docmanagers connection
            self._query("SELECT pg_terminate_backend(%s)", (self.connector.doc_managers[0].postgres.get_backend_pid(),))
            _id = self.conn.test.test1.insert_one({'name': 'retry'}).inserted_id
            assert_soon(lambda: self._count() == 1)
        finally:
            postgresql_doc_manager.time = time

        doc = self.conn.test.test1.find_one({"_id": _id})
        self.assertEqual([doc], [item['document'] for item in self._query("""SELECT document FROM "test.test1";""")])

    def test_remove(self):
        """Tests remove operations."""
        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: self._count() == 1)
        self.conn['test']['test'].delete_one({'name': 'paulie'})
        assert_soon(lambda: self._count() == 0)

    def test_update(self):
        """Test update operations."""
        # Insert
        doc = {"a": 0}
        self.conn.test.test.insert_one(doc)

        def check_update(update_spec):
            updated = self.conn.test.test.find_and_modify({"a": 0}, update_spec, new=True)
            assert_soon(lambda: updated == self._query()[0]['document'])

        # Update by adding a field
        check_update({"$set": {"b": [{"c": 10}, {"d": 11}]}})

        # Update by setting an attribute of a sub-document beyond end of array.
        check_update({"$set": {"b.10.c": 42}})

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

        # Update by adding new bucket to list
        check_update({"$push": {"b": {"e": 12}}})

        # Update by changing an entire sub-document
        check_update({"$set": {"b.0": {"e": 4}}})

        # Update by adding a sub-document
        check_update({"$set": {"b": {"0": {"c": 100}}}})

        # Update whole document
        check_update({"a": 0, "b": {"1": {"d": 10000}}})

    def test_rollback(self):
        """Test behavior during a MongoDB rollback.

        We force a rollback by adding a doc, killing the primary,
        adding another doc, killing the new primary, and then
        restarting both.
        """
        primary_conn = self.repl_set.primary.client()

        # This doc can be picked up in the collection dump
        self.conn.test.test.insert_one({'name': 'paul'})
        self.assertEqual(1, self.conn.test.test.find({'name': 'paul'}).count())
        assert_soon(lambda: self._count() == 1)

        # This doc is definitely not picked up by collection dump
        self.conn.test.test.insert_one({'name': 'pauly'})

        self.repl_set.primary.stop(destroy=False)

        new_primary_conn = self.repl_set.secondary.client()

        admin = new_primary_conn.admin
        assert_soon(lambda: admin.command("isMaster")['ismaster'])
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert_one, {'name': 'pauline'})
        assert_soon(lambda: self._count() == 3)
        result_set_2 = self.conn.test.test.find_one({'name': 'pauline'})
        result_set_1 = self._query("""SELECT document FROM "test.test" WHERE _id = %s""", (str(result_set_2["_id"]),))[0]['document']
        self.assertEqual(result_set_2, result_set_1)
        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        self.repl_set.secondary.start()

        assert_soon(lambda: self._count() == 2)
        find_cursor = retry_until_ok(self.conn.test.test.find)
        self.assertEqual(retry_until_ok(find_cursor.count), 2)

        result_set_2 = {doc['_id']: doc for doc in self.conn.test.test.find({})}
        result_set_1 = {item['document']['_id']: item['document'] for item in self._query()}
        self.assertEqual(result_set_2, result_set_1)

    def test_bad_int_value(self):
        self.conn.test.test.insert_one({
            'inf': float('inf'), 'nan': float('nan'),
            'still_exists': True})
        assert_soon(lambda: self._count() > 0)
        for doc in self._query():
            doc = doc['document']
            self.assertNotIn('inf', doc)
            self.assertNotIn('nan', doc)
            self.assertTrue(doc['still_exists'])


if __name__ == '__main__':
    unittest.main()
