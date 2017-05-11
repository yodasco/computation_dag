from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
import computation_dag


def spark_conf(num_shuffle_partitions=4):
    conf = SparkConf()
    conf.set('spark.sql.shuffle.partitions', num_shuffle_partitions)
    conf.set("spark.hadoop.mapred.output.compress", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec",
             "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    conf.set("spark.sql.broadcastTimeout", "1200")
    return conf


if __name__ == '__main__':
    sc = SparkContext(appName='test_application', conf=spark_conf())
    ctx = HiveContext(sc)
    tda = computation_dag.TextDataAdapter(ctx, 'test_data.txt')
    n1 = computation_dag.TrivialNode(tda, 'node1')
    df = n1.compute()
    df.show()
