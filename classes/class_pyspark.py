import json, os, re, sys
from typing import Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class SparkClass:

    def __init__(self, config):
        self.config = config
    
    def sparkStart(self):
        print(self.config)