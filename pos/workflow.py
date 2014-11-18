#!/usr/bin/python
# -*- coding=utf-8
import gzip
import json
import os
import re

import luigi
import luigi.hadoop
import luigi.hdfs
import luigi.format
import luigi.hive
import preprocessing
from preprocessing import custom_table_exists, get_hive_files

# Attach necessary modules for inclusion in jar.
luigi.hadoop.attach(preprocessing)

class Main(luigi.hadoop.JobTask):
    """
    Main class of this data flow. Summarizes the results from other tasks.
    """

    def requires(self):
        # Defines prerequisites and input.
        tasks = [ComputeRevenuePerQuarterPerUser(), ComputeTopCategoriesPerUser()]
        paths = list(get_hive_files('revenue_per_quarter_per_user')) + list(get_hive_files('top_categories_per_user'))
        for path in paths:
            tasks.append(preprocessing.InputText(path))
        return tasks


    def output(self):
        return luigi.hdfs.HdfsTarget('/out')
        #return luigi.LocalTarget('out')

    def mapper(self, line):
        # We get both (user_id, category, rank) tuples as well as (user_id, quarter, total_revenue) tuples as input.
        # This job is used to merge these values together by user_id and display a summary as required by the assignment.
        data = line.split('\001')

        if data[1] in map(str,range(1,5)):
            # Emit (user_id, (tuple string)) values.
            yield data[0], 'revenue\t%s\t%s' % tuple(data[1:])
            # We detect this to be a 'quarter' field. There are of course other ways to do this (use Hive).
            # But I wanted to demonstrate I can also do a reduce-side join in MapReduce without using tools like Hive.
        elif data[2] == '1':
            # Filter the category that ended 1st in the rankings (i.e. which is the most popular among a user).
            yield data[0], 'popular\t%s' % data[1]

    def reducer(self, key, values):
        """
        Reduce the (user_id, mangled string tuple) tuple to a readable summary.
        """

        summary = {
            'Q1': '\N',
            'Q2': '\N',
            'Q3': '\N',
            'Q4': '\N',
        }
        for value in values:
            data = value.split('\t')
            if data[0] == 'popular':
                summary['Popular'] = data[1]
                continue

            # Now it should be a revenue tuple.
            summary['Q%s' % data[1]] = data[2]



        yield key, '\t'.join([v for k,v in sorted(summary.items())])



class ComputeTopCategoriesPerUser(luigi.hive.HiveQueryTask):
    """
    Computes a table that contains the categories per user in decreasing order of popularity (rank 1 is most popular):
     (user_id, category, rank).
    """

    def requires(self):
        return preprocessing.Preprocessing()

    def query(self):
        return "DROP TABLE IF EXISTS top_categories_per_user; " \
               "CREATE TABLE top_categories_per_user AS " \
               "SELECT q.user_id, q.category, rank() OVER (PARTITION BY q.user_id ORDER BY q.volume DESC) AS rank " \
               "FROM (SELECT t.user_id, p.category, SUM(t.quantity) AS volume " \
               "      FROM transactions t " \
               "      JOIN products p " \
               "      ON p.id=t.product_id " \
               "      GROUP BY t.user_id, p.category) q  " \
               "ORDER BY q.user_id ASC;"

    def complete(self):
        return custom_table_exists('top_categories_per_user')

class ComputeRevenuePerQuarterPerUser(luigi.hive.HiveQueryTask):
    """
    Computes a table that contains the revenues per quarter per user: (user_id, quarter, total_revenue).
    """

    def requires(self):
        return preprocessing.Preprocessing()

    def query(self):
        return "DROP TABLE IF EXISTS revenue_per_quarter_per_user; " \
               "CREATE TABLE revenue_per_quarter_per_user AS " \
               "SELECT a.user_id, a.quarter, SUM(a.revenue) AS total_revenue " \
               "FROM (SELECT q.user_id, q.quarter, q.volume * p.price AS revenue " \
               "      FROM (SELECT t.user_id, t.product_id, ceil(month(t.date)/3) AS quarter, SUM(t.quantity) AS volume " \
               "            FROM transactions t " \
               "            GROUP BY t.user_id, t.product_id, ceil(month(t.date) / 3)) q " \
               "      JOIN products p ON q.product_id=p.id) a " \
               "GROUP BY a.user_id, a.quarter;"

    def complete(self):
        return custom_table_exists('revenue_per_quarter_per_user')

if __name__ == '__main__':
    luigi.run()
