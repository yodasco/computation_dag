# computation_dag

## Synopsis
SPARK computation framework.

## Motivation
A framework for composing displaying testing and dbuggining complex `Spark`
computation flows.

## Examples
This pakage is provided with a full working example.
To run the example localy: `FLAVOR=local make all`.
To print the computation graph to a pdf file: `make example.pdf`
Below are code fragments of the example.
1. Creating a computation node from CSV input file.
```python
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
```
2. Create a computation node, which does actual processing:
```python
match_node = ComputationNode(
    ctx,
    match_animals_to_noises,
    'match_node')
match_node.\
    add_dependency(animals_node).\
    add_dependency(noises_node)
graph.add_node(match_node)
```
Where `match_animals_to_noises` is a function with the following prototype:
```python
def match_animals_to_noises(ctx, df_animals, df_noises):
    # Implementation goes here.
    # ctx is of type HiveContext, both df_animals and df_noises are of type
    # pyspark.sql.DataFrame.
```

## Instalation
TBD
