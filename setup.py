import re
from setuptools import setup

test_suite = "tests"
tests_require = ["mongo-orchestration>= 0.6.7, < 1.0", "requests>=2.5.1", "testing.postgresql>=1.3.0"]

try:
    with open("README.rst", "r") as fd:
        long_description = fd.read()
except IOError:
    long_description = None  # Install without README.rst

with open("mongo_connector/doc_managers/postgresql_doc_manager.py", "r") as fd:
    data = fd.read()
version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", data, re.M)
if not version_match:
    raise RuntimeError("Unable to find version string.")
version = version_match.group(1)

setup(
    name='postgresql-doc-manager',
    version=version,
    maintainer='Cureatr',
    description='PostgreSQL plugin for mongo-connector',
    long_description=long_description,
    platforms=['any'],
    author='Andrew Wason',
    author_email='developers@cureatr.com',
    url='https://github.com/cureatr/postgresql-doc-manager',
    install_requires=['mongo-connector >= 2.5.0', "psycopg2>=2.6.0,<3.0.0"],
    packages=["mongo_connector", "mongo_connector.doc_managers"],
    license="Apache License, Version 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
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
