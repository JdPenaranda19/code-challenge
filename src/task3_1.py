import os

def load_iris_to_hdfs():
    os.system('curl -L "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data" -o iris.csv')
    os.system('hdfs dfs -copyFromLocal -f iris.csv /tmp/iris.csv')
    os.system('rm iris.csv')

if __name__ == '__main__':
    load_iris_to_hdfs()