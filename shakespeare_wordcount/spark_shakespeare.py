import os
import re
import shutil
import string

from pyspark.sql.session import SparkSession

BASE_OUTPUT_DIRECTORY = os.getcwd()
WORDCOUNT_OUTPUT = 'wordcount_interview_test'
OUTPUT_DIRECTORY = '/'.join([BASE_OUTPUT_DIRECTORY, WORDCOUNT_OUTPUT])
SHAKESPEARE_TEXT_URIS = '{}/*.txt'.format('/'.join([os.path.dirname(__file__), 'data']))

REGEXPS = (
    r"""
    (?:\w+(?:[\-']\w+))  # Compound Words with hyphens (a-b) or apostrophes (wouldn't)
    |
    (?:[^\w])  # Words without apostrophes or hyphens
    """,
)


class SparkContextManager:
    """ Just a basic wrapper on sparksession object to avoid call explicitly stop() function """

    def __init__(self, app_name, master):
        """
        Build a sparksession in a context-manager fashion.
        :param app_name: Name of the app.
        :param master: Deployment options.
        """
        self._spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(master) \
            .getOrCreate()

    def __enter__(self):
        return self._spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._spark.stop()


def clean_directory(dir_path):
    """
    Clean directory if already exists in order to avoid `FileAlreadyExistsException` (Hadoop's inheritance)
    :param dir_path: path to directory
    """

    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


def shakespeare_wordcount():
    """
    Computes the number of times a word appears in shakespeare texts.

    Wordcount algorithm:
        [textFile] :: Read every file and return a RDD (Resilient Distributed Dataset) of strings.
        [flatMap] :: For every word in every file, split it and flat the result into 1D list.
        [map] :: Remove whitespaces (if there are any).
        [filter] :: Filter words avoiding punctuation signs and empty words (i.e. '').
        [map] :: Map every word in a tuple (word, 1), where <int> will be the number of occurrences for that word.
        [reduceByKey] :: Count the occurrences of each word reducing by key.
        [sortBy] :: Sort words in descending occurrence order (just for showing the results, but not needed).
    """

    punctuation_characters = list(string.punctuation)
    text_regex = re.compile(r"""(%s)""" % "|".join(REGEXPS), re.VERBOSE | re.I | re.UNICODE | re.DOTALL)

    with SparkContextManager(app_name='Shakespeare_wordcount', master='local[*]') as spark:
        wordcount = spark.sparkContext.textFile(SHAKESPEARE_TEXT_URIS) \
            .flatMap(lambda text: text_regex.split(text.lower())) \
            .map(lambda word: word.strip()) \
            .filter(lambda word: word not in punctuation_characters and word) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda w1, w2: w1 + w2) \
            .sortBy(lambda x: x[1], ascending=False)

        # Save the result in one file (by default, spark splits data across 'n-partition' files)
        wordcount.coalesce(1).saveAsTextFile(OUTPUT_DIRECTORY)

        # Print the total number of words
        print("Total number of words: {:d}".format(wordcount.map(lambda x: x[1]).sum()))


if __name__ == '__main__':
    clean_directory(OUTPUT_DIRECTORY)
    shakespeare_wordcount()

"""
Run with:
 $ PYSPARK_PYTHON=<path_to_python>/bin/python SPARK_HOME=<path_to_spark>/ <path_to_spark>/bin/spark-submit
   orbitalads/spark_shakespeare.py
"""
