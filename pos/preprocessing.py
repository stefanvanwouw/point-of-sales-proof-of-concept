#!/usr/bin/python
# -*- coding=utf-8
import os
import luigi
import luigi.hadoop
import luigi.hdfs
import luigi.format
import luigi.hive
import mysql.connector
import settings

# Attach required modules for inclusion in jar.
luigi.hadoop.attach(mysql.connector)
luigi.hadoop.attach(settings)


class Preprocessing(luigi.Task):
    """
    Defines all preprocessing dependencies of the data flow.
    """

    def requires(self):
        return [CreateTransactionTable(),
                ExportMySQLToHive(source_table='products', destination='/input/products/products',
                                                schema="DROP TABLE IF EXISTS products; "
                                                       "CREATE EXTERNAL TABLE products "
                                                       "(id INT, name STRING, category STRING, price DOUBLE) "
                                                       "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "
                                                       "LOCATION '/input/products';")]

    def complete(self):
        return custom_table_exists('products') and custom_table_exists('transactions')

class ExportMySQLToHive(luigi.Task):
    """
    Export a MySQL table to Hive. You have to provide the CREATE TABLE statement in the schema parameter.
    """
    source_table = luigi.Parameter()
    destination = luigi.Parameter()
    schema = luigi.Parameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.destination, format=luigi.format.Gzip)

    def run(self):
        db = mysql.connector.connect(host=settings.MYSQL_DB_HOST, # your host, usually localhost
                                     user=settings.MYSQL_DB_USER, # your username
                                     password=settings.MYSQL_DB_PASS, # your password
                                     database=settings.MYSQL_DB_DB) # name of the data base

        cur = db.cursor()

        # Max uncompressed size of data returned by this query is 50MB for products table (because of the 99999 product limitation).
        # Other options are to use a query on a specific range of products in the respective mapreduce jobs.
        # I prefer to process everything from HDFS though.
        # A third option is putting the product database in a DistributedCache.
        cur.execute('SELECT * FROM %s;' % self.source_table)
        local_file = '%s' % self.source_table
        with open(local_file, 'wb') as out_file:
            for row in cur.fetchall():
                out_file.write('%s\n' % '\t'.join(map(str,row)))


        hdfs_client = luigi.hdfs.client
        destination_dir = os.path.dirname(self.destination)

        if not hdfs_client.exists(destination_dir):
            hdfs_client.mkdir(destination_dir)
        hdfs_client.put(local_file, self.destination)
        luigi.hive.run_hive_cmd(self.schema)



class CreateTransactionTable(luigi.hive.HiveQueryTask):
    """
    Create a clean Transaction Hive table.
    """

    def requires(self):
        return PreprocessTransactions(source='/input/transactions_raw', destination='/input/transactions')

    def complete(self):
        return custom_table_exists('transactions')

    def query(self):
        return "DROP TABLE IF EXISTS transactions; " \
               "CREATE EXTERNAL TABLE transactions " \
               "(date STRING, order_id INT, product_id INT, user_id INT, acc_mgr_id INT, quantity INT) " \
               "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " \
               "LOCATION '/input/transactions';"



class PreprocessTransactions(luigi.hadoop.JobTask):
    """
    Hive does not support multi Byte characters as field delimiter.
    So we first get rid of this multi-byte character. We also get rid of the headers.
    This is less job-time efficient, but saves a lot of development and maintenance time by using Hive for data processing,
    rather than a custom MapReduce job.
    """
    source = luigi.Parameter()
    destination = luigi.Parameter()

    def requires(self):
        tasks = []

        paths = get_file_paths(self.source)
        for path in paths:
            tasks.append(InputTextGzip(path))
        return tasks

    def output(self):
        return luigi.hdfs.HdfsTarget(self.destination, format=luigi.format.Gzip)

    def mapper(self, line):
        data = line.split("þ")

        # We get rid of the headers in the transaction log files.
        if 'Date' in data[0]:
            return


        yield data[0], '\t'.join(data[1:])


def custom_table_exists(table):
    # The default HiveTableTarget implementation is currently malfunctioning.
    try:
        return luigi.hive.client.table_exists(table)
    except luigi.hive.HiveCommandError as e:
        if 'Table not found' not in e.err:
            raise e
    return False

def get_file_paths(dir):
    if luigi.hdfs.client.exists(dir):
        return luigi.hdfs.client.listdir(dir, ignore_directories=True, recursive=True)
    return []

def get_hive_files(table):
    return get_file_paths('/user/hive/warehouse/%s' % table)

class InputTextGzip(luigi.ExternalTask):
    """
    Create a dependency on a GZIPed input file.
    """
    path = luigi.Parameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.path, format=luigi.format.Gzip)

class InputText(luigi.ExternalTask):
    """
    Create a dependency on an input file.
    """
    path = luigi.Parameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.path, format=luigi.format.Gzip)
