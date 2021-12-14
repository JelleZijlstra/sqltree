"""
Fixit rule for formatting SQL.
"""


import libcst as cst
from fixit import CstLintRule
from fixit import InvalidTestCase as Invalid
from fixit import ValidTestCase as Valid
from libcst.helpers import get_full_name_for_node

from sqltree.parser import ParseError

from .formatter import format


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
        )
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

    def visit_Call(self, node: cst.Call) -> None:
        # TODO format specific calls
        pass

    def visit_Assign(self, node: cst.Assign) -> None:
        full_name = get_full_name_for_node(node.targets[0].target)
        if full_name == "sql" and isinstance(node.value, cst.SimpleString):
            query = node.value.evaluated_value
            try:
                formatted = format(query)
            except ParseError as e:
                self.report(node, message=str(e))
            else:
                # TODO escaping, indent to current escape level, preserve prefix
                replacement = f'"""\n{formatted}"""'
                if replacement != node.value.value:
                    new_str = node.value.with_changes(value=replacement)
                    self.report(node.value, replacement=new_str)