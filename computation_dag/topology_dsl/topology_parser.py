import argparse
import copy
import re

WhiteSpacePattern = re.compile('\s+')


def update_location(str_input, loc):
    if not str_input:
        return loc
    m = WhiteSpacePattern.match(str_input)
    if m is None:
        return loc
    prefix = m.group(0)
    return loc + prefix.count('\n')


# Lexer.
class Literal:
    def __init__(self, name, expr, ty):
        self.name = name
        self.expr = expr
        self.pattern = re.compile(expr)
        self.ty = ty
        self.val = ''

    def __copy__(self):
        him = Literal(self.name, self.expr, self.ty)
        him.val = copy.copy(self.val)
        return him

    def __str__(self):
        return self.val

    def first(self):
        return self.ty

    def parse(self, str_input, location):
        self.location = update_location(str_input, location)
        str_input = str_input.lstrip()
        m = self.pattern.match(str_input)
        if m is None:
            return (False, str_input)
        self.val = m.group(0)
        mlen = len(self.val)
        return (True, str_input[mlen:])

    def visit(self, cb, ctx):
        cb(self, ctx)


class Rule:
    def __init__(self, name, *right_side):
        self.name = name
        self.right_side = list(right_side)

    def parse(self, str_input, location):
        self.location = location
        for var in self.right_side:
            (success, str_input) = var.parse(str_input, self.location)
            self.location = var.location
            if not success:
                return (False, str_input)
        return (True, str_input)

    def first(self):
        return self.right_side[0].first()

    def visit(self, cb, ctx):
        for rs in self.right_side:
            rs.visit(cb, ctx)
        cb(self, ctx)


class ArityRule:
    def __init__(self, name, rule_factory):
        self.name = name
        self.rule_factory = rule_factory
        self.parsed_rules = list()

    def parse(self, str_input, location):
        self.location = location
        while True:
            rule = self.rule_factory()
            (success, str_input) = rule.parse(str_input, self.location)
            r1 = rule.right_side[1]
            self.location = rule.location
            if not success:
                break
            # Copy the parsed rule to support Sema.
            self.parsed_rules.append(rule)
        return (True, str_input)

    def __str__(self):
        ret = 'R({})'.format(self.name)
        for v in self.parsed_rules:
            ret += str(v) + ' '
        return ret

    def first(self):
        return self.rule.first()

    def visit(self, cb, ctx):
        for rule in self.parsed_rules:
            cb(rule, ctx)


# Grammar specific.
NodeDeclTy = 0
VarDeclTy = 1
OpenCurlyTy = 2
CloseCurlyTy = 3
OpenParenTy = 4
CloseParenTy = 5
DeclTy = 6
GraphTy = 7
ArrowTy = 8
CommaTy = 10
SemiColonTy = 11
ColonTy = 12
StringLiteralTy = 13


def createVarLiteral():
    return Literal('var_name', '[a-zA-Z_][a-zA-Z0-9]*', VarDeclTy)


def topology_parser():

    # Literals
    node = Literal('Computation literal', 'ComputationNode|InputNode',
                   NodeDeclTy)
    open_curly = Literal('open curly', '\{', OpenParenTy)
    close_curly = Literal('close curly', '\}', CloseCurlyTy)
    open_paren = Literal('open parenthesis', '\(', OpenParenTy)
    close_paren = Literal('close parenthesis', '\)', CloseParenTy)
    decl_block = Literal('decl keyword', 'decl', DeclTy)
    graph_block = Literal('graph keyword', 'graph', GraphTy)
    arrow = Literal('arrow keyword', '->', ArrowTy)
    comma = Literal('comma', ',', CommaTy)
    semi_colon = Literal('semicolon', ';', SemiColonTy)
    colon = Literal('colon', ':', ColonTy)
    string = Literal('string literal', '"[^"]+"', StringLiteralTy)
    # Rules.
    # NodeDecl -> node var_name '(' params ')' ';'
    first_param = Rule('param', createVarLiteral(), colon, string)

    def _create_args_param():
        return Rule('comma-param', comma, first_param)
    args_param = ArityRule('comma-param*', _create_args_param)
    params = Rule('parameters', first_param, args_param)

    def _create_node_decl():
        return Rule('Node declaration', node, createVarLiteral(), open_paren,
                    params, close_paren, semi_colon)
    # NodeDecls -> NodeDecl+
    NodeDecls = ArityRule('NodeDecl*', _create_node_decl)
    # Decl -> 'decl' '{' NodeDecls+ '}'
    Decl = Rule('declaration block', decl_block, open_curly, NodeDecls,
                close_curly)

    def _create_edge_decl():
        return Rule('edge', createVarLiteral(), arrow, createVarLiteral(),
                    semi_colon)
    EdgeDecls = ArityRule('edge*', _create_edge_decl)
    # Graph -> 'graph' '{' EdgeDecl+ '}'
    Graph = Rule('Graph block', graph_block, open_curly, EdgeDecls,
                 close_curly)
    return Rule('S', Decl, Graph)


def print_error_msg(line, reminder):
    print "Error: line %d: '%s'" % (line, reminder)


if __name__ == '__main__':
    # Grammar.
    dsl_parser = topology_parser()
    # Arguments.
    arg_parser = argparse.ArgumentParser(
            description='Computation topology syntactic parser test.')
    arg_parser.add_argument('input_file', help='Input file.')
    args = arg_parser.parse_args()
    with file(args.input_file) as f:
        contents = f.read()
        # Parse the root, the number it the first line indicator.
        (success, reminder) = dsl_parser.parse(contents, 1)
        if not success:
            print_error_msg(dsl_parser.location, reminder)
    if success:
        print 'Parse finished successfully'
    else:
        print 'Parse failed.'
