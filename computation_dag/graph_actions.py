'''
Generates dot graph from from the given graph.
'''
import os


def _replace_ilegal_chars(attribute):
    illegals = [';', '[', ']']
    for illegal in illegals:
        attribute = attribute.replace(illegal, '_')
    return attribute


def generate_dot(graph, output_path):
    '''
    Generates a 'dot' file of the computation topology of the given graph.
    '''
    f = open(output_path, 'w')
    nodes = dict()

    def _new_node(node):
        nodes[node] = len(nodes)

    fname = os.path.basename(output_path)
    fname = os.path.splitext(fname)[0]
    f.write('digraph %s {\n' % fname.replace('.', '_'))
    graph.dfs(_new_node)
    for node in nodes:
        color = 'blue' if node.is_output_node() else 'black'
        shape = 'box' if node.is_root_node() else 'ellipse'
        node_name = _replace_ilegal_chars(node.get_name())
        f.write('\t%d [label=%s color=%s shape=%s];\n'
                % (nodes[node], node_name, color, shape))

    for node in nodes:
        for child in node.each_child():
            f.write('\t%d -> %d\n' % (nodes[child], nodes[node]))
    f.write('}\n')
    f.close()


def trace_run(graph, out_path):
    '''
    Writes each the output of every computation node to a specified folder.
    '''

    def _write_node(node):
        node.compute().write.json(path=os.path.join(out_path, node.get_name()),
                                  mode='overwrite')

    graph.dfs(_write_node)
