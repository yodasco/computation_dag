'''
Semantic checks for grammar rule.
'''
import argparse
from topology_parser import topology_parser


def sema_check_node_decl(node_decl, ctx):
    # NodeDecl -> node_ty var_name ( params* );
    sym_name = str(node_decl.right_side[1])
    if sym_name in ctx.sym_table:
        err = (node_decl.location,
               'Node {} is already defined.'.format(sym_name))
        ctx.add_sema_error(err)
        return
    ctx.sym_table[sym_name] = node_decl


def sema_check_graph(graph_decl, ctx):
    pass


FUNCTION_ROUTE = {
        'Node declaration': sema_check_node_decl,
        'Graph block': sema_check_graph
}


def sema_check(node, ctx):
    name = node.name
    if name not in FUNCTION_ROUTE:
        return
    FUNCTION_ROUTE[name](node, ctx)


class SemaContext:
    def __init__(self):
        self.sym_table = dict()
        self.errors = list()

    def add_sema_error(self, err):
        self.errors.append(err)

    def has_errors(self):
        return self.errors

    def print_errors(self):
        for (loc, msg) in self.errors:
            print 'Line {}: {}'.format(loc, msg)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
            description='Computation topology semantic parser test.')
    arg_parser.add_argument('input_file', help='Input file.')
    args = arg_parser.parse_args()
    dsl_parser = topology_parser()
    with file(args.input_file) as f:
        contents = f.read()
    (success, reminder) = dsl_parser.parse(contents, 1)
    assert success
    ctx = SemaContext()
    dsl_parser.visit(sema_check, ctx)
    if ctx.has_errors():
        ctx.print_errors()
