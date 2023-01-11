"""Setup.py for the Astronomer sample Airflow provider package. Built from datadog provider package for now."""

from setuptools import find_packages, setup

with open("README.rst", "r") as fh:
    long_description = fh.read()

"""Perform the package sqream_blue-provider setup."""
setup(
    name='airflow-provider-sqream-blue',
    version="0.0.1",
    description='About Apache Airflow - A platform to programmatically author, schedule, and monitor workflows.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=sqream_blue.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['sqream_blue', 'sqream_blue.hooks','sqream_blue.operators'],
    install_requires=['apache-airflow>=2.0', 'apache-airflow-providers-common-sql==1.3.2', 'pysqream-blue==1.0.18'],
    setup_requires=['setuptools', 'wheel'],
    author='SQream',
    author_email='info@sqream.com',
    url='https://github.com/SQream/apache-airflow-providers-sqream-blue',
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires='~=3.7',
)