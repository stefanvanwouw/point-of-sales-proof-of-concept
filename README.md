=======
point-of-sales-proof-of-concept
===============================

Proof of Concept for processing transaction logs using Hadoop MapReduce, Hive and Luigi.

- The 'pos' package contains all the code for processing a point of sales transaction log. Calculating the most popular product category per user, together with the revenue per quarter per user.

- The preprocessing.py contains the pipeline used for cleaning the input data.

- The workflow.py file contains the actual data processing related to the problem statement.

- The setup.py module contains all the package dependencies.

- The client.cfg file contains Luigi configuration.

Make sure to have the transaction log files present in the /input/transactions_raw directory on HDFS, together with the 'products' catalog in a MySQL database of which the credentials are configured in settings.py.

The schema for the MySQL product catalog looks like this:

```
CREATE TABLE products (
  id INT(32) PRIMARY KEY,
  name VARCHAR(255),
  category VARCHAR(255),
  price DOUBLE
);

INSERT INTO products VALUES 
(1, 'Banana', 'Fruit', 1.00),
(2, 'Apple', 'Fruit', 0.85),
(3, 'Raspberry', 'Fruit', 1.65),
(4, 'Chocolate sprinkles', 'Decoration', 1.00),
.
.
(9998, 'Windscreen wipers', 'Car accessories', 33.00),
(9999, 'Rear windscreen wipers', 'Car accessories', 35.00);

```

The format for the /input/transactions_raw files is as follows (they are assumed to be GZIP compressed):

```
DateþOrderIDþProductIDþUserIDþAccMngrIDþQuantity
2013-01-01þ10000þ1þ1þ1þ10
2013-01-01þ10000þ2þ1þ1þ5
2013-01-01þ10001þ1þ2þ1þ1
.
.
.
2013-12-31þ99992þ9999þ666þ1þ10
```


After installing Hive, Hadoop MapReduce (YARN) and Luigi, and adapting the client.cfg, start with:
```
python workflow.py Main --local-scheduler
```
