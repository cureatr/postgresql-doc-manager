Getting Started
---------------

This package is a document manager for `mongo-connector <https://github.com/mongodb-labs/mongo-connector>`__.
It stores mongodb documents as PostgreSQL jsonb documents with a GIN index.
This is useful for reporting on a mongodb database, reports can be run against PostgreSQL while taking advantage of JOINs aon json fields across collections/tables.

Each mongodb collection is replicated into PostgreSQL as a table of the same name, each row of the table has a ``document`` jsonb field and an ``_id`` primary key field.

Documents are stored using bson.json_util.dumps.
With psycopg2, register a handler (globally or on a connection) to restore them when querying::

  psycopg2.extras.register_default_jsonb(conn, loads=bson.json_util.loads)

Installation
~~~~~~~~~~~~

The easiest way to install postgresql-doc-manager is with
`pip <https://pypi.python.org/pypi/pip>`__::

  pip install postgresql-doc-manager

You can also install the development version of postgresql-doc-manager
manually::

  git clone https://github.com/cureatr/postgresql-doc-manager
  cd postgresql-doc-manager
  python setup.py install

You may have to run ``python setup.py install`` with ``sudo``, depending
on where you're installing mongo-connector and what privileges you have.

Running the tests
-----------------
Requirements
~~~~~~~~~~~~

1. PostgreSQL 9.5

2. Mongo Orchestration

   Mongo Connector runs MongoDB on its own using another tool called `Mongo Orchestration <https://github.com/mongodb/mongo-orchestration>`__. This package should install automatically if you run ``python setup.py test``, but the Mongo Orchestration server still needs to be started manually before running the tests::

      mongo-orchestration start

   will start the server. To stop it::

      mongo-orchestration stop

   The location of the MongoDB server can be set in orchestration.config. For more information on how to use Mongo Orchestration, or how to use it with different arguments, please look at the Mongo-Orchestration README.

3. Environment variables

   There are a few influential environment variables that affect the tests. These are:

   - ``DB_USER`` is the username to use if running the tests with authentication enabled.
   - ``DB_PASSWORD`` is the password for the above.
   - ``MONGO_PORT`` is the starting port for running MongoDB. Future nodes will be started on sequentially increasing ports.
   - ``MO_ADDRESS`` is the address to use for Mongo Orchestration (i.e. hostname:port)

All the tests live in the `tests` directory.

Running tests on the command-line
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The tests take care of setting up and tearing down MongoDB clusters on their own.
If you are already running mongodb on port 27017, then specify MONGO_PORT=27018 when running tests.

You can run all the tests with one command (this works in all supported Python versions)::

  python setup.py test

In addition, you can be more selective with which tests you run (in Python > 2.6 only)! For example, if you only wanted to run the postgresql doc manager tests::

  python -m unittest tests.test_postgresql_doc_manager

Error messages
~~~~~~~~~~~~~~

Some of the tests are meant to generate lots of ``ERROR``-level log messages, especially the rollback tests. mongo-connector logs exceptions it encounters while iterating the cursor in the oplog, so we see these in the console output while MongoDB clusters are being torn apart in the tests. As long as all the tests pass with an `OK` message, all is well.
