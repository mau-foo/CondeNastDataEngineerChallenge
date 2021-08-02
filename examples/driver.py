"""
If you plan on using PySpark, please use the requirements.txt file at the root directory
of the project for a list of dependencies.

You are free to the code copy from "import findspark" to "spark.read()", we
wanted to ensure that you are able to focus on the business logic and not
installing Spark related items.
"""

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("testApp").getOrCreate()
csv_path = "../data/circuits.csv"
df = spark.read.format("csv").load(csv_path).limit(10)
print(df.collect())