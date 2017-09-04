'''
Usage example for computation graph framework.
'''
import argparse
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import HiveContext
import graph_actions as GA
from topology_builder import TrivialNode, ComputationNode, ComputationGraph
from topology_builder import CsvDataAdapter

APP_NAME = 'Usage Example'
NUM_PARTITION = 10


def bootstrap():
    conf = SparkConf()
    conf.set('spark.sql.shuffle.partitions', NUM_PARTITION)
    conf.set("spark.hadoop.mapred.output.compress", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec",
             "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    conf.set("spark.sql.broadcastTimeout", "1200")
    return conf


def match_animals_to_noises(ctx, df_animals, df_noises):
    ALIAS = '_'
    return df_animals.join(
        df_noises.select(col('Animal').alias(ALIAS), 'Sound'),
        col('Animal') == col(ALIAS),
        'inner')


def create_example_flow(ctx, path_animals, path_noises):
    graph = ComputationGraph()
    # Animals node.
    animals_adapter = CsvDataAdapter(
        ctx,
        path_animals,
        sep=',',
        header=True)
    animals_node = TrivialNode(
        animals_adapter,
        'animals_node')
    graph.add_node(animals_node)
    # Noises node.
    noises_adapter = CsvDataAdapter(
        ctx,
        path_noises,
        sep=',',
        header=True)
    noises_node = TrivialNode(
        noises_adapter,
        'noises_node')
    graph.add_node(noises_node)
    # Match node.
    match_node = ComputationNode(
        ctx,
        match_animals_to_noises,
        'match_node')
    match_node.\
        add_dependency(animals_node).\
        add_dependency(noises_node)
    graph.add_node(match_node)
    return graph


def write_output(df, path):
    df.coalesce(1)
    df.write.csv(path=path, mode='overwrite')


def run_spark(args):
    conf = bootstrap()
    sc = SparkContext(appName=APP_NAME, conf=conf)
    ctx = HiveContext(sc)
    graph = create_example_flow(ctx, args.input_animals, args.input_noises)
    nodes = graph.get_output_nodes()
    if (len(nodes) != 1):
        raise Exception('Unexpected graph output')
    output_node = nodes[0]
    if args.dot_output:
        GA.generate_dot(graph, args.dot_output)
        return
    write_output(output_node.compute(), args.output_match)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # Input.
    parser.add_argument('--input_animals', required=True)
    parser.add_argument('--input_noises', required=True)
    # Output.
    parser.add_argument('--output_match', required=True)
    parser.add_argument('--dot_output', required=False)
    run_spark(parser.parse_args())
