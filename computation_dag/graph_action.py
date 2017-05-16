'''
Generates dot graph from from the given graph.
'''
import os


def _replace_ilegal_chars(attribute):
    illegals = [';', '[', ']']
    for illegal in illegals:
        attribute = attribute.replace(illegal, '_')
    return attribute


def _create_node_dict(graph):
    nodes = dict()

    def _new_node(node):
        nodes[node] = len(nodes)
    graph.dfs(_new_node)

    return nodes


def generate_dot(graph, output_path):
    '''
    Generates a 'dot' file from the given graph.
    '''
    f = open(output_path, 'w')
    fname = os.path.basename(output_path)
    fname = os.path.splitext(fname)[0]
    f.write('digraph %s {\n' % fname.replace('.', '_'))
    nodes = _create_node_dict(graph)
    for node in nodes:
        color = 'blue' if node.is_output_node() else 'black'
        shape = 'box' if node.is_root_node() else 'ellipse'
        node_name = _replace_ilegal_chars(node.get_name())
        f.write('\t%d [label=%s color=%s shape=%s];\n'
                % (nodes[node], node_name, color, shape))

    for node in nodes:
        for child in node.each_child():
            f.write('\t%d -> %d\n' % (nodes[node], nodes[child]))
    f.write('}\n')
    f.close()


def _write_indented(f, s, indent_level):
    f.write('\t' * indent_level)
    f.write(s)


def generate_tpl(graph, output_path):
    '''
    Generates a 'tpl (topology file)' from the given graph.
    '''
    indent_level = 0
    f = open(output_path, 'w')
    nodes = _create_node_dict(graph)
    # Declaration block.
    _write_indented(f, 'decl {\n', indent_level)
    indent_level += 1
    for node in nodes:
        params = dict()
        if node.is_root_node():
            node_decl = 'InputNode'
            # Path.
            adapter = node.data_adapter
            params['path'] = adapter.path
            params['format'] = adapter.get_format_string()
        else:
            node_decl = 'ComputationNode'
            params['function'] = node.func.__name__
            params['file'] = node.func.__code__.co_filename
        # Write the parameters.
        _write_indented(f, '{} {}('.format(node_decl, node.name), indent_level)
        str_params = ['{}:"{}"'.format(k, v) for k, v in params.items()]
        f.write(','.join(str_params))
        f.write(');\n')
    f.write('}\n')
    # Graph block.
    indent_level -= 1
    _write_indented(f, 'graph {\n', indent_level)
    indent_level += 1
    for node in nodes:
        lname = node.name
        for child in node.each_child():
            rname = child.name
            _write_indented(f,
                            '{} -> {};\n'.format(lname, rname),
                            indent_level)
    f.write('}\n')
