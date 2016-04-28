from setuptools import setup
import sys

test_suite = "tests"
tests_require = ["mongo-orchestration>= 0.2, < 0.4", "requests>=2.5.1", "testing.postgresql>=1.3.0"]

if sys.version_info[:2] == (2, 6):
    # Need unittest2 to run unittests in Python 2.6
    tests_require.append("unittest2")
    test_suite = "unittest2.collector"

try:
    with open("README.rst", "r") as fd:
        long_description = fd.read()
except IOError:
    long_description = None  # Install without README.rst

setup(
    name='postgresql-doc-manager',
    version='0.1.0',
    maintainer='Cureatr',
    description='PostgreSQL plugin for mongo-connector',
    long_description=long_description,
    platforms=['any'],
    author='Andrew Wason',
    author_email='developers@cureatr.com',
    url='https://github.com/cureatr/postgresql-doc-manager',
    install_requires=['mongo-connector >= 2.3.0', "psycopg2>=2.6.0,<3.0.0"],
    packages=["mongo_connector", "mongo_connector.doc_managers"],
    license="Apache License, Version 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX"
    ],
    keywords=['mongo-connector', "mongodb", "postgres", "postgresql"],
    test_suite=test_suite,
    tests_require=tests_require,
)
