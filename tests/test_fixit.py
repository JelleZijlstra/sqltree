from fixit import add_lint_rule_tests_to_module

# fixit injects this class again here under a different name,
# which confuses pyanalyze
from sqltree.fixit import SqlFormatRule as _SQLFormatRule

add_lint_rule_tests_to_module(globals(), rules={_SQLFormatRule})
