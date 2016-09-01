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


def compare_dicts(a, b, path=[]):
    seen_items = set()
    for item in sorted(a) + sorted(b):
        if item in seen_items:
            continue
        seen_items.add(item)

        a_value = a.get(item)
        b_value = b.get(item)
        if a_value == b_value:
            continue

        itempath = path + [item]
        itemdesc = " ".join(itempath)
        if b_value is None:
            if isinstance(a_value, dict) and len(a_value) == 1:
                yield "-{} {} {}".format(itemdesc, *a_value.popitem())
            else:
                yield "-{}".format(itemdesc)
        elif a_value is None:
            if isinstance(b_value, dict) and len(b_value) == 1:
                yield "+{} {} {}".format(itemdesc, *b_value.popitem())
            else:
                yield "+{}".format(itemdesc)
        elif not isinstance(a_value, dict) or not isinstance(b_value, dict):
            yield "-{} {}".format(itemdesc, a_value)
            yield "+{} {}".format(itemdesc, b_value)
        else:
            yield from compare_dicts(a_value, b_value, path=itempath)


def query_for_comparison(topic, target, cursor, sql, args=[]):
    cursor.execute(sql, args)
    for row in cursor.fetchall():
        key = "{} {!r}".format(topic, row.pop("key"))
        key_target = target.setdefault(row.pop("table"), {}).setdefault(key, {})
        key_target.update(row)


def compare_schema(a_db, b_db, schemas, ignore_partitions):
    a_cursor = a_db.cursor(cursor_factory=RealDictCursor)
    b_cursor = b_db.cursor(cursor_factory=RealDictCursor)

    if not schemas:
        schemaset = set()
        a_cursor.execute("SELECT nspname FROM pg_namespace WHERE left(nspname, 3) <> 'pg_'")
        schemaset.update(row["nspname"] for row in a_cursor.fetchall())
        b_cursor.execute("SELECT nspname FROM pg_namespace WHERE left(nspname, 3) <> 'pg_'")
        schemaset.update(row["nspname"] for row in b_cursor.fetchall())
        schemas = sorted(schemaset)

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

    a_info = {}
    b_info = {}

    # quick and dirty table / column comparison
    a_cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = ANY(%s)", [schemas])
    a_db_cols = a_cursor.fetchall()
    b_cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = ANY(%s)", [schemas])
    b_db_cols = b_cursor.fetchall()

    def cols_for_comparison(cols, ignored_parts, target):
        ignored_properties = {"table_catalog", "udt_catalog", "ordinal_position", "dtd_identifier"}
        for col in cols:
            table = "{}.{}".format(col["table_schema"], col["table_name"])
            if table in ignored_parts:
                continue
            key = "Column {!r}".format(col["column_name"])
            for prop, val in col.items():
                if prop not in ignored_properties and val is not None:
                    key_target = target.setdefault(table, {}).setdefault(key, {})
                    key_target[prop] = val

    cols_for_comparison(a_db_cols, a_parts, a_info)
    cols_for_comparison(b_db_cols, b_parts, b_info)

    # quick and dirty index comparison
    index_sql = """
        SELECT schemaname || '.' || tablename AS table, indexname AS key, indexdef AS definition
            FROM pg_indexes
            WHERE schemaname = ANY(%s)
                AND NOT (schemaname || '.' || tablename = ANY(%s))
        """
    query_for_comparison("Index", a_info, a_cursor, index_sql, [schemas, list(a_parts | b_parts)])
    query_for_comparison("Index", b_info, b_cursor, index_sql, [schemas, list(a_parts | b_parts)])

    # quick and dirty constraint comparison
    const_sql = """
        SELECT n.nspname || '.' || cl.relname AS table, co.conname AS key, pg_get_constraintdef(co.oid, true) AS definition
            FROM pg_constraint AS co
                JOIN pg_namespace AS n ON (co.connamespace = n.oid)
                JOIN pg_class AS cl ON (co.conrelid = cl.oid)
            WHERE n.nspname = ANY(%s)
                AND NOT (n.nspname || '.' || cl.relname = ANY(%s))
        """
    query_for_comparison("Constraint", a_info, a_cursor, const_sql, [schemas, list(a_parts | b_parts)])
    query_for_comparison("Constraint", b_info, b_cursor, const_sql, [schemas, list(a_parts | b_parts)])

    yield "--- {}".format(a_db.dsn)
    yield "+++ {}".format(b_db.dsn)

    all_rels = sorted(set(a_info) | set(b_info))
    for rel in all_rels:
        if a_info.get(rel) == b_info.get(rel):
            continue

        yield "@@ Relation {}".format(rel)
        if rel not in a_info:
            yield "+Relation {!r}".format(rel)
            continue
        if rel not in b_info:
            yield "+Relation {!r}".format(rel)
            continue

        yield from compare_dicts(a_info[rel], b_info[rel])


def compare(*, a_conn_str, b_conn_str, schemas, ignore_partitions):
    count = 0
    with psycopg2.connect(a_conn_str) as a_db, psycopg2.connect(b_conn_str) as b_db:
        for issue in compare_schema(a_db=a_db, b_db=b_db, schemas=schemas, ignore_partitions=ignore_partitions):
            if issue.startswith("@@"):
                count += 1
            print(issue)
    print("Found differences in {} relations".format(count))
    return count


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=False, action="append")
    parser.add_argument("--ignore-partitions", action="store_true")
    parser.add_argument("connection_string_1")
    parser.add_argument("connection_string_2")
    pargs = parser.parse_args(args)

    diffs = compare(
        a_conn_str=pargs.connection_string_1,
        b_conn_str=pargs.connection_string_2,
        schemas=pargs.schema,
        ignore_partitions=pargs.ignore_partitions,
    )
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
