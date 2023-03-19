from setuptools import setup, find_packages

setup(
    name='sendgrid_spark_email_automation',
    version='1.0.0',
    url='https://github.com/liam-clifford/sendgrid_spark_email_automation',
    author='Liam Clifford',
    author_email='liamclifford4@gmail.com',
    description='This Python library provides a simple way to send emails using SendGrid's API and automates the process of logging email records in Spark.',
    packages=find_packages(),
    install_requires=[
        'sendgrid==6.9.7',
        'pyspark==3.3.2',
        'requests==2.28.2',
        'numpy==1.24.2',
        'pandas==1.5.3'
    ],
)
