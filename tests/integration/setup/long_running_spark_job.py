#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""A Spark job that keeps its driver (and executors) alive indefinitely.

Used by integration tests that need the driver/executor pods to stay in the
"Running" phase while assertions (e.g. mesh reachability) are performed. The job
runs until the driver pod is deleted externally.
"""

import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("long-running-test-job").getOrCreate()

# Touch the cluster so an executor is scheduled, then idle until killed.
spark.sparkContext.parallelize(range(1), 1).count()

while True:
    time.sleep(10)
