# Code challenge

This repository contains the implementation of some of the PySpark components using different datasets found in the internet. In particular, PySpark components used are the ones shown bellow

1. Spark RDD API (*task1_\*.py*)
2. Spark Dataframe API (*task2_\*.py*)
3. Spark ML (*task3_\*.py*)

Where each one of the *task\*_\*.py* files is present in the **src** folder and their results can be found in the **out** folder. In addition, the *utilities.py* file can also be found under the **src** folder. This python script cointains two functions which are used throughout the execution of the different python scripts. These functions are *write_file* and *merge_dictionaries_with_sum*.

## Execution

A Dataproc single node cluster was used in the execution of each one of the python scripts in this repository. Therefore, the command used was the following 

`spark-submit --master yarn --deploy-mode client task*_*.py`

However, you can feel free and choose the option that best fits for you when trying it on your own.

