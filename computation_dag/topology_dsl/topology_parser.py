import re
import argparse

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
        self.pattern = re.compile(expr)
        self.ty = ty

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


class ArityRule:
    def __init__(self, name, rule):
        self.name = name
        self.rule = rule

    def parse(self, str_input, location):
        self.location = location
        while True:
            (success, str_input) = self.rule.parse(str_input, self.location)
            self.location = self.rule.location
            if not success:
                break
        return (True, str_input)

    def first(self):
        return self.rule.first()


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


def topology_grammar():
    # Literals
    node = Literal('Computation literal', 'ComputationNode|InputNode',
                   NodeDeclTy)
    var_name = Literal('var_name', '[a-zA-Z_][a-zA-Z0-9]*', VarDeclTy)
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
    first_param = Rule('param', var_name, colon, string)
    args_param = Rule('comma-param', comma, first_param)
    params = Rule('parameters', first_param,
                  ArityRule('comma-param*', args_param))
    NodeDecl = Rule('Node declaration', node, var_name, open_paren, params,
                    close_paren,
                    semi_colon)
    # NodeDecls -> NodeDecl+
    NodeDecls = ArityRule('NodeDecl*', NodeDecl)
    # Decl -> 'decl' '{' NodeDecls+ '}'
    Decl = Rule('declaration block', decl_block, open_curly, NodeDecls,
                close_curly)
    EdgeDecl = Rule('edge', var_name, arrow, var_name, semi_colon)
    EdgeDecls = ArityRule('edge*', EdgeDecl)
    # Graph -> 'graph' '{' EdgeDecl+ '}'
    Graph = Rule('graph block', graph_block, open_curly, EdgeDecls,
                 close_curly)
    return Rule('S', Decl, Graph)


def print_error_msg(line, reminder):
    print "Error: line %d: '%s'" % (line, reminder)


if __name__ == '__main__':
    # Grammar.
    dsl_parser = topology_grammar()
    # Arguments.
    arg_parser = argparse.ArgumentParser(
            description='Computation topology parser.')
    arg_parser.add_argument('input_file', help='Input file.')
    args = arg_parser.parse_args()
    with file(args.input_file) as f:
        contents = f.read()
        (success, reminder) = dsl_parser.parse(contents, 1)
        if not success:
            print_error_msg(dsl_parser.location, reminder)
    if success:
        print 'Parse finished successfully'
    else:
        print 'Parse failed.'
