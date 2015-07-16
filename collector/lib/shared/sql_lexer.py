# -*- coding: utf-8 -*-

# Copyright (C) 2008 Andi Albrecht, albrecht.andi@gmail.com
# Copyright (C) 2014 Zack Weinberg, zackw@panix.com
#
# Originally from python-sqlparse, based on pygments' SqlLexer.
# BSD License: http://www.opensource.org/licenses/bsd-license.php.

"""Specialized SQL lexer whose primary function is to canonicalize the
format of CREATE TABLE and CREATE INDEX statements, so schemas can be
checked for consistency."""

__all__ = ['canonicalize', 'tokenize', 'is_constraint']

import re

# Token classes

T_TYPEMASK   = 0x0F
T_INVALID    = 0x00  # lexical error
T_WHITE      = 0x01  # white space
T_STANDALONE = 0x02  # never merges with anything
T_WORD       = 0x03  # may merge with other T_WORDs
T_STRING     = 0x04  # may merge with other T_STRINGs
T_OPERATOR   = 0x05  # may merge with other T_OPERATORs

T_FLAGMASK   = 0xF0
T_KEYWORD    = 0x10  # flag: uppercase me
T_ENDSTMT    = 0x20  # flag: force CR after

# This keyword list taken from the SQLite documentation.

KEYWORDS = [
    'ABORT', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALTER', 'ANALYZE', 'AND', 'AS',
    'ASC', 'ATTACH', 'AUTOINCREMENT', 'BEFORE', 'BEGIN', 'BETWEEN', 'BLOB',
    'BY', 'CASCADE', 'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'COMMIT',
    'CONFLICT', 'CONSTRAINT', 'CREATE', 'CROSS', 'CURRENT_DATE',
    'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'DATABASE', 'DEFAULT', 'DEFERRABLE',
    'DEFERRED', 'DELETE', 'DESC', 'DETACH', 'DISTINCT', 'DROP', 'EACH',
    'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXCLUSIVE', 'EXISTS', 'EXPLAIN',
    'FAIL', 'FOR', 'FOREIGN', 'FROM', 'FULL', 'GLOB', 'GROUP', 'HAVING', 'IF',
    'IGNORE', 'IMMEDIATE', 'IN', 'INDEX', 'INDEXED', 'INITIALLY', 'INNER',
    'INSERT', 'INSTEAD', 'INTEGER', 'INTERSECT', 'INTO', 'IS', 'ISNULL',
    'JOIN', 'KEY', 'LEFT', 'LIKE', 'LIMIT', 'MATCH', 'NATURAL', 'NO', 'NOT',
    'NOTNULL', 'NULL', 'OF', 'OFF', 'OFFSET', 'ON', 'OR', 'ORDER', 'OUTER',
    'PLAN', 'PRAGMA', 'PRIMARY', 'QUERY', 'RAISE', 'REAL', 'RECURSIVE',
    'REFERENCES', 'REGEXP', 'REINDEX', 'RELEASE', 'RENAME', 'REPLACE',
    'RESTRICT', 'RIGHT', 'ROLLBACK', 'ROW', 'SAVEPOINT', 'SELECT', 'SET',
    'TABLE', 'TEMP', 'TEMPORARY', 'TEXT', 'THEN', 'TO', 'TRANSACTION',
    'TRIGGER', 'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VACUUM', 'VALUES',
    'VIEW', 'VIRTUAL', 'WHEN', 'WHERE', 'WITH', 'WITHOUT',
]

GRAMMAR = [
    (r'--.*?(?:\r\n|\r|\n|$)', T_WHITE),  # single line comment
    (r'(?s)/\*.*?\*/',         T_WHITE),  # multiline comment
    (r'\s+',                   T_WHITE),  # plain white space

    # These characters always stand for themselves.
    (r'[()\[\],]',             T_STANDALONE),
    (r';',                     T_STANDALONE|T_ENDSTMT),

    # Punctuators.
    (r'[-+*/<>=~!%^&|]+',      T_OPERATOR),

    # Words.
    (KEYWORDS,                 T_WORD|T_KEYWORD),
    (r'[^\W\d_]\w*\b',         T_WORD),

    # Numbers and variables are treated as words for merge purposes.
    (r'[0-9]+(?:\.[0-9]*)?(?:[eE]-?[0-9]+)?', T_WORD),
    (r'[0-9]*\.[0-9]+(?:[eE]-?[0-9]+)?',      T_WORD),
    (r'\?[0-9]*',                             T_WORD),
    (r'[$@#:][^\W\d_]\w*\b',                  T_WORD),

    # Characters that *could* merge with words but are also used
    # standalone.
    (r'[$@#:.]',                              T_WORD),

    # These are all *lexically* strings.  We don't try to track
    # each separately.
    (r'`(?:``|[^`])*`', T_STRING),
    (r'"(?:""|[^"])*"', T_STRING),
    (r"'(?:''|[^'])*'", T_STRING),

    # These characters have no business appearing at all (in isolation).
    (r'[\x00-\x1f\\_{}\x7f-\u10ffff]', T_INVALID),

    # Incomplete tokens (for error recovery at EOF)
    (r'/\*[^*]*',                                T_INVALID),
    (r'/\*[^*]*\*+(?:[^*/][^*]*\*+)*',           T_INVALID),
    (r'/\*[^*]*\*+(?:[^*/][^*]*\*+)*[^*/][^*]*', T_INVALID),

    (r'`(?:``|[^`])*`',                          T_INVALID),
    (r'"(?:\"\"|[^\"])',                         T_INVALID),
    (r"'(?:''|[^'])*",                           T_INVALID),

    (r'[0-9]+(?:\.[0-9]*)?[eE]-?',               T_INVALID),
    (r'[0-9]*\.[0-9]+[eE]-?',                    T_INVALID),
]

def compile_grammar(grammar):
    flags = re.IGNORECASE
    compiled = []
    for rule in grammar:
        if isinstance(rule[0], str):
            compiled.append((re.compile(rule[0], flags).match, rule[1]))
        else:
            # sort longest-to-shortest to ensure getting the longest match
            rx = ("(?:" +
                  "|".join(sorted(rule[0], key=len, reverse=True)) +
                  ")\\b")
            compiled.append((re.compile(rx, flags).match, rule[1]))

    def apply_decoration(fn):
        fn._tokens = compiled
        return fn
    return apply_decoration

@compile_grammar(GRAMMAR)
def tokenize(text):
    """Split ``text`` into (tokentype, text) pairs."""
    tokens = tokenize._tokens
    pos = 0
    limit = len(text)
    while pos < limit:
        for rexmatch, action in tokens:
            m = rexmatch(text, pos)
            if m:
                # print rex.pattern
                value = m.group()
                if hasattr(action, '__call__'):
                    ttype, value = action(value)
                    yield ttype, value
                else:
                    yield action, value
                pos = m.end()
                break
        else:
            raise ValueError("unmatchable text at position {}: {}..."
                             .format(pos, text[pos:(pos+20)]))

def canonicalize(text):
    """Reformat TEXT, a sequence of SQL statements, into a canonical form
       suitable for comparison with other such statements."""
    result = []
    last_nonwhitespace = T_STANDALONE
    pending_whitespace = False

    for tag, value in tokenize(text):
        ttype  = tag & T_TYPEMASK
        tflags = tag & T_FLAGMASK

        if tflags & T_KEYWORD:
            value = value.upper()
        if tflags & T_ENDSTMT:
            value += '\n'

        if ttype == T_INVALID:
            raise ValueError("invalid token: " + repr(value))

        if ttype == T_WHITE:
            pending_whitespace = True
            continue

        if (ttype != T_STANDALONE and ttype == last_nonwhitespace
            and pending_whitespace):
          result.append(' ')
        pending_whitespace = False
        last_nonwhitespace = ttype
        result.append(value)

    return ''.join(result)

def is_constraint(column):
    return not not is_constraint._re.match(column)

is_constraint._re = re.compile(
    r'(?:CONSTRAINT\s+\w+\s*)?(?:(?:PRIMARY|FOREIGN)\s+KEY|UNIQUE|CHECK)\s*\(',
    re.IGNORECASE)
