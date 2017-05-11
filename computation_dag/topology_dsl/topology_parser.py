import re


# Lexer.
class Literal:
    def __init__(self, expr, ty):
        self.pattern = re.compile(expr)
        self.ty = ty

    def is_terminal(self):
        return True

    def first(self):
        return self.ty

    def parse(self, str_input):
        m = self.pattern.match(val)
        if m is None:
            return (False, 0)
        self.val = m.group(0)
        return (True, len(self.val))


class Rule:
    def __init__(self, *right_side):
        self.right_side = list(right_side)

    def is_terminal(self):
        return False

    def parse(self, str_input):
        for var in self.right_side:
            (success, mlen) = var.parse(str_input)
            if not success:
                return False
            str_input = str_input[mlen:]
        return True

    def first(self):
        return self.right_side[0].first()


class ArityRule:
    def __init__(self, rule):
        self.rule = rule

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


if __name__ == '__main__':
    # Literals
    node = Literal('ComputationNode|InputNode', NodeDeclTy)
    var_name = Literal('[a-zA-Z_][a-zA-Z0-9]*', VarDeclTy)
    open_curly = Literal('{', OpenParenTy)
    close_curly = Literal('}', CloseCurlyTy)
    open_paren = Literal('(', OpenParenTy)
    close_paren = Literal(')', CloseParenTy)
    decl_block = Literal('decl', DeclTy)
    graph_block = Literal('graph', GraphTy)
    arrow = Literal('->', ArrowTy)
    comma = Literal(',', CommaTy)
    semi_colon = Literal(';', SemiColonTy)
    colon = Literal(':', ColonTy)
    string = Literal('\"[^"]\"', StringLiteralTy)
    # Rules.

    Decl = Rule(decl_block, open_paren, *NodeDecls, close_paren)
    Graph = None
    s = Rule(Decl, Graph)
