#!/usr/bin/python3
"""
schema_compare - compare schemas of two databases

Copyright (C) 2016, https://aiven.io/
This file is under the Apache License, Version 2.0.
See http://www.apache.org/licenses/LICENSE-2.0 for details.

Compare the tables, columns, indexes and constraints of two PostgreSQL databases.

"""

from psycopg2.extras import RealDictCursor
import argparse
import psycopg2
import sys


def compare_dicts(topic, a_db_dict, b_db_dict, a_name, b_name):
    a_set = set(a_db_dict)
    b_set = set(b_db_dict)

    for key in sorted(b_set - a_set):
        yield "{t} {k} found in {b!r}, missing from {a!r}".format(t=topic, k=key, a=a_name, b=b_name)

    for key in sorted(a_set - b_set):
        yield "{t} {k} found in {a!r}, missing from {b!r}".format(t=topic, k=key, a=a_name, b=b_name)

    for key in sorted(a_set & b_set):
        a_db_info = a_db_dict[key]
        b_db_info = b_db_dict[key]
        if a_db_info != b_db_info:
            if isinstance(a_db_info, dict) and isinstance(b_db_info, dict):
                yield from compare_dicts("{} {}".format(topic, key), a_db_info, b_db_info, a_name, b_name)
            else:
                yield "{t} {k} is {ia!r} in {a!r}, {ib!r} in {b!r}".format(t=topic, k=key, a=a_name, b=b_name, ia=a_db_info, ib=b_db_info)


def compare_queries(topic, a_cursor, b_cursor, a_name, b_name, query, args=[]):
    a_cursor.execute(query, args)
    a_db_items = {r["key"]: r["val"] for r in a_cursor.fetchall()}
    b_cursor.execute(query, args)
    b_db_items = {r["key"]: r["val"] for r in b_cursor.fetchall()}
    yield from compare_dicts(topic, a_db_items, b_db_items, a_name, b_name)


def compare_schema(a_db, b_db, schemas, ignore_partitions):
    a_cursor = a_db.cursor(cursor_factory=RealDictCursor)
    b_cursor = b_db.cursor(cursor_factory=RealDictCursor)

    a_cursor.execute("SELECT current_database() AS db")
    a_name = a_cursor.fetchone()["db"]
    b_cursor.execute("SELECT current_database() AS db")
    b_name = b_cursor.fetchone()["db"]

    if a_name == b_name:
        a_name = "{} (a)".format(a_name)
        b_name = "{} (b)".format(b_name)

    # partition ignoring is based on names for now
    if ignore_partitions:
        query = """
            SELECT n.nspname || '.' || c.relname AS partname
                FROM pg_inherits AS i
                    JOIN pg_class AS c ON (c.oid = i.inhrelid)
                    JOIN pg_namespace AS n ON (n.oid = c.relnamespace)
            """
        a_cursor.execute(query)
        a_parts = {row["partname"] for row in a_cursor.fetchall()}
        b_cursor.execute(query)
        b_parts = {row["partname"] for row in b_cursor.fetchall()}
    else:
        a_parts, b_parts = set(), set()

    # quick and dirty table / column comparison
    a_cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = ANY(%s)", [schemas])
    a_db_cols = a_cursor.fetchall()
    b_cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = ANY(%s)", [schemas])
    b_db_cols = b_cursor.fetchall()

    def cols_for_comparison(cols, ignored_parts):
        ignored_properties = {"table_catalog", "udt_catalog", "ordinal_position", "dtd_identifier"}
        for col in cols:
            key = "{}.{}".format(col["table_schema"], col["table_name"])
            if key in ignored_parts:
                continue
            key = "{}.{}".format(key, col["column_name"])
            yield key, {k: v for k, v in col.items() if k not in ignored_properties and v is not None}

    a_db_col_dict = dict(cols_for_comparison(a_db_cols, a_parts))
    b_db_col_dict = dict(cols_for_comparison(b_db_cols, b_parts))
    yield from compare_dicts("Column", a_db_col_dict, b_db_col_dict, a_name, b_name)

    # quick and dirty index comparison
    index_sql = """
        SELECT schemaname || '.' || tablename || '/' || indexname AS key, indexdef AS val
            FROM pg_indexes
            WHERE schemaname = ANY(%s)
                AND NOT (schemaname || '.' || tablename = ANY(%s))
        """
    yield from compare_queries("Index", a_cursor, b_cursor, a_name, b_name, index_sql, [schemas, list(a_parts | b_parts)])

    # quick and dirty constraint comparison
    const_sql = """
        SELECT n.nspname || '.' || cl.relname || '/' || co.conname AS key, pg_get_constraintdef(co.oid, true) AS val
            FROM pg_constraint AS co
                JOIN pg_namespace AS n ON (co.connamespace = n.oid)
                JOIN pg_class AS cl ON (co.conrelid = cl.oid)
            WHERE n.nspname = ANY(%s)
                AND NOT (n.nspname || '.' || cl.relname = ANY(%s))
        """
    yield from compare_queries("Constraint", a_cursor, b_cursor, a_name, b_name, const_sql, [schemas, list(a_parts | b_parts)])


def compare(*, a_conn_str, b_conn_str, schemas, ignore_partitions):
    count = 0
    with psycopg2.connect(a_conn_str) as a_db, psycopg2.connect(b_conn_str) as b_db:
        for issue in compare_schema(a_db=a_db, b_db=b_db, schemas=schemas, ignore_partitions=ignore_partitions):
            print(issue)
            count += 1
    print("{} differences found".format(count))
    return count


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--a-db", required=True, metavar="CONN_STR")
    parser.add_argument("--b-db", required=True, metavar="CONN_STR")
    parser.add_argument("--schema", required=True, action="append")
    parser.add_argument("--ignore-partitions", action="store_true")
    pargs = parser.parse_args(args)

    return compare(a_conn_str=pargs.a_db, b_conn_str=pargs.b_db, schemas=pargs.schema, ignore_partitions=pargs.ignore_partitions)


if __name__ == "__main__":
    sys.exit(main())
