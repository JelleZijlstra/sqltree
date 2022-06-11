"""
Fixit rule for formatting SQL.
"""


import libcst as cst
from fixit import CstLintRule, InvalidTestCase as Invalid, ValidTestCase as Valid
from libcst.helpers import get_full_name_for_node

from .formatter import format
from .parser import ParseError


class SqlFormatRule(CstLintRule):
    """
    Uses sqltree to format SQL queries.
    """

    MESSAGE = "Reformat SQL"

    VALID = [
        Valid(
            '''
            sql = """
                SELECT *
                FROM x
            """
            '''
        ),
        Valid(
            '''
            def f():
                sql = """
                    SELECT *
                    FROM x
                """
            def g():
              x

            def weirdly_indented():
                if x:
                   sql = """
                       SELECT y
                       FROM z
                   """
            '''
        ),
    ]

    INVALID = [
        Invalid(
            "sql = 'select  * from x'",
            line=1,
            column=7,
            expected_replacement='''
            sql = """
                SELECT *
                FROM x
            """''',
        )
    ]

    current_indent: int = 0
    default_indent: int = 0

    def visit_Module(self, node: cst.Module) -> None:
        self.default_indent = len(node.default_indent.replace("\t", " " * 4))

    def visit_IndentedBlock(self, node: cst.IndentedBlock) -> None:
        self.current_indent += self._indent_of(node)

    def leave_IndentedBlock(self, node: cst.IndentedBlock) -> None:
        self.current_indent -= self._indent_of(node)

    def _indent_of(self, node: cst.IndentedBlock) -> int:
        if node.indent is not None:
            return len(node.indent.replace("\t", " " * 4))
        else:
            return self.default_indent

    def visit_Call(self, node: cst.Call) -> None:
        # TODO format specific calls
        pass

    def visit_Assign(self, node: cst.Assign) -> None:
        full_name = get_full_name_for_node(node.targets[0].target)
        if full_name == "sql" and isinstance(node.value, cst.SimpleString):
            query = node.value.evaluated_value
            try:
                formatted = format(query, indent=self.current_indent + 4)
            except ParseError as e:
                self.report(node, message=str(e))
            else:
                # TODO escaping, preserve prefix
                replacement = f'"""{formatted}"""'
                if replacement != node.value.value:
                    new_str = node.value.with_changes(value=replacement)
                    self.report(node.value, replacement=new_str)
