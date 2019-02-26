# Shakespeare WordCount

Shakespeare wordcount for KingLear, Othello and Romeo&Juliet literary works using Apache Spark (PySpark) 2.3.2. prebuilt for Apache Hadoop 2.7 and later.

## Installation
You need to have installed [Apache Spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz) in order to run this script. Once you have Spark installed, you will need to set the following `environment variables`:

- `PYSPARK_PYTHON`: to run the same python version in workers and executors.
- `SPARK_HOME`: to set where is installed spark and binaries.

## How can I use?
Guessing your python binary is in `/home/myvirtualenv/bin/python` and your uncompressed spark folder is in `/home/spark-2.4.0-bin-hadoop2.7/`, you can run `spark_shakespeare.py` as follow:

```bash
  shakespeare_wordcount$ PYSPARK_PYTHON=/home/myvirtualenv/bin/python SPARK_HOME=/home/spark-2.4.0-bin-hadoop2.7/ /home/spark-2.4.0-bin-hadoop2.7/bin/spark-submit spark_shakespeare.py
```

Running the script will save in a folder (`wordcount_interview_test`) one file called `part-00000` with the number of times a given word appears in the texts.
