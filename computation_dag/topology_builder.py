'''
Computation topology builder.
Allows one to build computation topologies, thus encapsulate computation's
logic from it's topology.
'''
import collections


class DataAdapter:
    def _validate_path(self):
        if (self.path is None) or (not self.path):
            err = 'path is null or empty'
            if self.name:
                err += " in Data Adapter '{}'".format(self.name)
            raise Exception(err)

    def _try_load(self, f, **kwargs):
        try:
            if isinstance(self.path, basestring):
                df = f(path=self.path, **kwargs)
                df.first()
            elif isinstance(self.path, collections.Iterable):
                frames = map(lambda the_path: f(path=the_path, **kwargs),
                             self.path)
                df = reduce(lambda l, r: l.union(r), frames)
                df.first()
            return df
            raise Exception('unsuported type {}'.format(type(self.path)))
        except Exception as e:
            raise Exception('{}: Path {} seems to be invalid.\n'
                            '{}'.
                            format(self.__class__.__name__,
                                   self.path,
                                   e))

    def get_data_frame():
        pass


class JsonDataAdapter(DataAdapter):
    def __init__(self, ctx, path, name=None, schema=None):
        self.ctx = ctx
        self.path = path
        self.name = name
        self._validate_path()
        self.schema = schema
        if ctx is None:
            raise Exception('None context')

    def get_data_frame(self):
        return self._try_load(self.ctx.read.json, schema=self.schema)


class MySqlDataAdapter(DataAdapter):
    def __init__(self, ctx, conn_string, schema, tbl_name):
        if ctx is None:
            raise Exception('None context')
        if (conn_string is None) or (not conn_string):
            raise Exception('connection string is null or empty')
        self.ctx = ctx
        self.conn_string = conn_string
        self.schema = schema
        self.tbl_name = tbl_name

    def load_data_from_mysql(self):
        dbt = '{schema}.{table}'.format(schema=self.schema,
                                        table=self.tbl_name)
        df = self.ctx.read.jdbc(self.conn_string,
                                dbt,
                                properties={'driver': 'com.mysql.jdbc.Driver'})
        return df

    def get_data_frame(self):
        df = self.load_data_from_mysql()
        try:
            df.first()
        except Exception as e:
            raise Exception('Loading table from MySql failed, for connection'
                            'string %s. %s' % (self.conn_string, e))
        return df


class TextDataAdapter(DataAdapter):
    def __init__(self, ctx, path):
        if ctx is None:
            raise Exception('None context')
        self.ctx = ctx
        self.path = path
        self._validate_path()

    def get_data_frame(self):
        return self.ctx.read.text(self.path)


class CsvDataAdapter(DataAdapter):
    def __init__(self, ctx, path, **kwargs):
        if ctx is None:
            raise Exception('None context')
        self.ctx = ctx
        self.path = path
        self.kwargs = kwargs
        self._validate_path()

    def get_data_frame(self):
        return self._try_load(self.ctx.read.csv, **self.kwargs)


class Node:
    def __init__(self, name):
        self.output_node = True
        self.name = name

    def compute():
        pass

    def is_output_node(self):
        '''
        To be overridden by derived classes.
        '''
        return self.output_node

    def set_output_node(self, val):
        self.output_node = val

    def get_name(self):
        return self.name

    def is_root_node(self):
        return False

    def each_child(self):
        return
        yield


class TrivialNode(Node):
    '''
    Nodes which require no previous computation.
    '''
    def __init__(self, data_adaptor, name=None):
        Node.__init__(self, name)
        self.data_adaptor = data_adaptor

    def compute(self):
        return self.data_adaptor.get_data_frame()

    def is_root_node(self):
        '''
        Indicates whether the given node has dependencies.
        Returns true is so, False otherwise.
        '''
        return True


class ComputationNode(Node):
    '''
    Computations that are dependent on the output of other computation(s).
    '''
    def __init__(self, ctx, func, name=None, force_output=False):
        Node.__init__(self, name)
        self.ctx = ctx
        self.func = func
        self.force_output = force_output
        self.dependencies = list()

    def add_dependency(self, node):
        self.dependencies.append(node)
        # Dependent node will be invoked by this node, no need for the graph to
        # invoke it explicitly.
        node.set_output_node(False)
        return self

    def compute(self):
        df_list = list()
        # Dependency.
        for dependency in self.dependencies:
            df_list.append(dependency.compute())
        return self.func(self.ctx, *df_list)

    def is_output_node(self):
        if self.force_output:
            return True
        return Node.is_output_node(self)

    def get_name(self):
        return self.name

    def __getitem__(self, idx):
        return IndexedComputationNode(self, idx)

    def each_child(self):
        for child in self.dependencies:
            yield child


class IndexedComputationNode(ComputationNode):
    '''
    To be used when one wants to 'hide' the implementation detail by which a
    node computation outputs more than one DataFrame.
    '''
    def __init__(self, node, idx):
        Node.__init__(self, node.name)
        self.node = node
        self.idx = idx

    def compute(self):
        return self.node.compute()[self.idx]

    def is_output_node(self):
        return self.node.is_output_node()

    def add_dependency(self, node):
        self.node.add_dependency(node)
        return self

    def get_name(self):
        return '{}[{}]'.format(ComputationNode.get_name(self), self.idx)

    def set_output_node(self, val):
        self.node.set_output_node(val)

    def each_child(self):
        return self.node.each_child()


class ComputationGraph():
    '''
    Represents a computation graph.
    At the moment pretty lame, in the future may prove useful for visualisation
    and graph validity (to prevent circular dependencies) and optimizations.
    '''
    def __init__(self):
        self.nodes = set()

    def add_node(self, node):
        '''
        Adds the given node to the graph.
        Input:
            node: node to be added.
        '''
        self.nodes.add(node)
        return node

    def get_output_nodes(self):
        '''
        Returns the nodes that need explicit activation (i.e., node with no
        outgoing edges).
        '''
        return filter(lambda n: n.is_output_node(), self.nodes)

    def dfs(self, node_action):
        '''
        Preforms a depth-first search traversal on the graph, applying the
        given node-action (pre-order) on each node encountered for the first
        time.
        '''
        visited = set()
        node_stack = self.get_output_nodes()
        while len(node_stack) > 0:
            node = node_stack.pop()
            if node in visited:
                continue
            node_action(node)
            visited.add(node)
            for child in node.each_child():
                if child in visited:
                    continue
                node_stack.append(child)
