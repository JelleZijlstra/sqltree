from fixit import add_lint_rule_tests_to_module
from sqltree.fixit import SqlFormatRule

add_lint_rule_tests_to_module(globals(), rules=[SqlFormatRule])
