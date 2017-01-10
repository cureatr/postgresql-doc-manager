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

"""PostgreSQL implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
PostgreSQL, storing documents as JSONB.
"""
import logging
import time
import numbers
import math
from contextlib import contextmanager

import psycopg2
import psycopg2.errorcodes
import psycopg2.extras
import bson
import bson.json_util

from mongo_connector import errors, compat
from mongo_connector.constants import DEFAULT_MAX_BULK
from mongo_connector.util import exception_wrapper
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

__version__ = "0.3.1"

log = logging.getLogger(__name__)

PARENT_TABLE = "mongodb_collections"


def exception_retry(f):
    def wrapped(self, *args, **kwargs):
        while True:
            try:
                return f(self, *args, **kwargs)
            except psycopg2.Error:
                if self.postgres.closed:
                    log.exception("Connection closed, reconnecting")
                    self._reconnect()
                    continue
                # Reraise as fatal error
                raise
    return wrapped


class BSONDocumentFormatter(DefaultDocumentFormatter):
    """Format document using bson.json_util rules, except for nan/inf"""
    def transform_value(self, value):
        if isinstance(value, dict):
            return self.format_document(value)
        elif isinstance(value, list):
            return [self.transform_value(v) for v in value]
        elif isinstance(value, numbers.Number):
            if math.isnan(value):
                raise ValueError("nan")
            elif math.isinf(value):
                raise ValueError("inf")
            return value
        elif compat.is_string(value) or isinstance(value, bool) or value is None:
            return value
        return bson.json_util.default(value)


class DocManager(DocManagerBase):
    """PostgreSQL implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    PostgreSQL, storing documents as JSONB
    """

    def __init__(self, url, unique_key='_id', chunk_size=DEFAULT_MAX_BULK, **kwargs):
        self.postgres = self._connect(url)
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self._formatter = BSONDocumentFormatter()

        # Create parent table that all collections inherit from
        # http://www.postgresql.org/docs/9.5/static/ddl-inherit.html
        with self._transaction() as cursor:
            cursor.execute(
                u"""CREATE TABLE IF NOT EXISTS {parent} ("{id}" text, _ts bigint, document jsonb);"""
                .format(parent=PARENT_TABLE, id=self.unique_key)
            )

    def _reconnect(self):
        while True:
            try:
                self.postgres = psycopg2.connect(self.postgres.dsn)
                return
            except psycopg2.Error as e:
                log.error("PostgreSQL reconnection error, retrying... (%s)", e)
                time.sleep(5)

    @exception_wrapper({psycopg2.Error: errors.ConnectionFailed})
    def _connect(self, dsn):
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
        return psycopg2.connect(dsn)

    @contextmanager
    def _transaction(self):
        with self.postgres, self.postgres.cursor() as cursor:
            yield cursor

    def _create_table(self, namespace):
        with self._transaction() as cursor:
            # Quote table name since mongodb collection names are case sensitive
            cursor.execute(
                u"""CREATE TABLE IF NOT EXISTS "{table}" ("{id}" text PRIMARY KEY) INHERITS ({parent});"""
                u"""CREATE INDEX IF NOT EXISTS "{table}_ts_idx" ON "{table}" (_ts DESC);"""
                .format(parent=PARENT_TABLE, table=namespace, id=self.unique_key)
            )

    def stop(self):
        pass

    @exception_retry
    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]

        if doc.get('dropDatabase'):
            for _db in self.command_helper.map_db(db):
                with self._transaction() as cursor:
                    cursor.execute(u"""SELECT tablename FROM pg_tables WHERE tablename LIKE '{db}.%';""".format(db=_db))
                    for r in cursor.fetchall():
                        table = r[0]
                        log.debug("Dropping table %s", table)
                        cursor.execute(u"""DROP TABLE "{table}";""".format(table=table))

        if doc.get('renameCollection'):
            from_t = self.command_helper.map_namespace(doc['renameCollection'])
            to_t = self.command_helper.map_namespace(doc['to'])
            if from_t and to_t:
                from_t = u"{}.{}".format(db, from_t)
                to_t = u"{}.{}".format(db, to_t)
                with self._transaction() as cursor:
                    cursor.execute(u"""ALTER TABLE "{from_t}" RENAME TO "{to_t}";""".format(from_t=from_t, to_t=to_t))

        if doc.get('create'):
            db, coll = self.command_helper.map_collection(db, doc['create'])
            if db and coll:
                table = u"{}.{}".format(db, coll)
                log.debug("Creating table %s", table)
                self._create_table(table)

        if doc.get('drop'):
            db, coll = self.command_helper.map_collection(db, doc['drop'])
            if db and coll:
                with self._transaction() as cursor:
                    table = u"{}.{}".format(db, coll)
                    log.debug("Dropping table %s", table)
                    cursor.execute(u"""DROP TABLE "{table}";""".format(table=table))

    @exception_wrapper({psycopg2.Error: errors.OperationFailed})
    def bulk_upsert(self, docs, namespace, timestamp):
        """Insert multiple documents into PostgreSQL."""
        # Bulk upsert doesn't create tables first
        self._create_table(namespace)
        with self._transaction() as cursor:
            for doc in docs:
                self._upsert(cursor, doc, namespace, timestamp)

    @exception_retry
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to document document_id."""
        with self._transaction() as cursor:
            cursor.execute(u"""SELECT document FROM "{table}" WHERE "{id}" = %s;""".format(table=namespace, id=self.unique_key), (compat.u(document_id),))
            result = cursor.fetchone()
            document = result[0] if result else {}
            updated = self.apply_update(document, update_spec)
            updated["_id"] = document_id
            self._upsert(cursor, updated, namespace, timestamp)
            return updated

    @exception_retry
    def upsert(self, doc, namespace, timestamp):
        """Insert a document into PostgreSQL."""
        with self._transaction() as cursor:
            self._upsert(cursor, doc, namespace, timestamp)

    def _upsert(self, cursor, doc, namespace, timestamp):
        doc_id = compat.u(doc["_id"])
        log.debug("Upsert %s into %s", doc_id, namespace)
        cursor.execute(u"""INSERT INTO "{table}" ("{id}", _ts, document) VALUES (%(id)s, %(ts)s, %(doc)s) """
                       u"""ON CONFLICT ("{id}") """
                       u"""DO UPDATE SET (_ts, document) = (%(ts)s, %(doc)s);""".format(table=namespace, id=self.unique_key),
                       {"id": doc_id, "ts": timestamp, "doc": psycopg2.extras.Json(self._formatter.format_document(doc))})

    @exception_retry
    def remove(self, document_id, namespace, timestamp):
        """Remove a document from PostgreSQL."""
        with self._transaction() as cursor:
            cursor.execute(u"""DELETE FROM "{table}" WHERE "{id}" = %s;""".format(table=namespace, id=self.unique_key),
                           (compat.u(document_id),))

    def search(self, start_ts, end_ts):
        """Query PostgreSQL for documents in a time range.

        This method is used to find documents that may be in conflict during
        a rollback event in MongoDB.
        """
        with self._transaction() as cursor:
            # Search all descendant tables using time range
            cursor.execute(u"""SELECT {parent}.tableoid::regclass, "{id}", _ts FROM {parent}* """
                           u"""WHERE _ts >= %s AND _ts <= %s;""".format(parent=PARENT_TABLE, id=self.unique_key),
                           (start_ts, end_ts))
            for ns, _id, _ts in cursor:
                yield {"_id": _id, "ns": ns.strip('"'), "_ts": _ts}

    def commit(self):
        pass

    @exception_retry
    def get_last_doc(self):
        """Get the most recently modified document from PostgreSQL.

        This method is used to help define a time window within which documents
        may be in conflict after a MongoDB rollback.
        """
        with self._transaction() as cursor:
            # Search all descendant tables using time range
            cursor.execute(u"""SELECT {parent}.tableoid::regclass, "{id}", _ts """
                           u"""FROM {parent}* ORDER BY _ts DESC LIMIT 1;""".format(parent=PARENT_TABLE, id=self.unique_key))
            result = cursor.fetchone()
            if result:
                ns, _id, _ts = result
                return {"_id": _id, "ns": ns.strip('"'), "_ts": _ts}
            return None
