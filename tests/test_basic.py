from pyspark import SparkContext
from pyspark.sql import HiveContext
from util import spark_conf
import computation_dag


if __name__ == '__main__':
    sc = SparkContext(appName='test_application', conf=spark_conf())
    ctx = HiveContext(sc)
    tda = computation_dag.TextDataAdapter(ctx, 'test_data.txt')
    n1 = computation_dag.TrivialNode(tda, 'node1')
    df = n1.compute()
    df.show()
