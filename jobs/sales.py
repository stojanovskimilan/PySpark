import logging
import json, os, re, sys
from typing import Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('py4j')

sys.path.insert(1, project_dir)
from classes import class_pyspark

def main(project_dir:str) -> None:
    """ Starts a Spark job """
    #class_pyspark.SparkClass(config={}).sparkStart()
    openFile(f"{project_dir}/json/sales.py")

def openFile(filepath:str):
    if isinstance(filepath, str) and os.path.exists(filepath):
        print(filepath)
    
if __name__ == '__main__':
    main(project_dir)
