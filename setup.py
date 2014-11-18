from distutils.core import setup

setup(
    name='pos',
    version='1.0',
    packages=['pos'],
    url='',
    license='MIT',
    author='Stefan van Wouw',
    author_email='stefanvanwouw@gmail.com',
    description='Example how to use Luigi + Hive for processing transaction log data.',
    requires=['luigi', 'mysql-connector-python', 'mechanize', 'tornado']
)
