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
import json
import os
import psycopg2
import sys


def gather_query_data(topic, target, cursor, sql, args=[]):
    cursor.execute(sql, args)
    for row in cursor.fetchall():
        key = "{} {!r}".format(topic, row.pop("key"))
        key_target = target.setdefault(row.pop("table"), {}).setdefault(key, {})
        key_target.update(row)


def gather_data(db, schemas, ignore_partitions):
    cursor = db.cursor(cursor_factory=RealDictCursor)

    if not schemas:
        cursor.execute("SELECT nspname FROM pg_namespace WHERE left(nspname, 3) <> 'pg_'")
        schemas = [row["nspname"] for row in cursor.fetchall()]

    # partition ignoring is based on names for now
    if ignore_partitions:
        query = """
            SELECT n.nspname || '.' || c.relname AS partname
                FROM pg_inherits AS i
                    JOIN pg_class AS c ON (c.oid = i.inhrelid)
                    JOIN pg_namespace AS n ON (n.oid = c.relnamespace)
            """
        cursor.execute(query)
        parts = {row["partname"] for row in cursor.fetchall()}
    else:
        parts = set()

    rels = {}
    cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = ANY(%s)", [schemas])
    cols = cursor.fetchall()

    ignored_properties = {"table_catalog", "udt_catalog", "ordinal_position", "dtd_identifier"}
    for col in cols:
        table = "{}.{}".format(col["table_schema"], col["table_name"])
        if table in parts:
            continue
        key = "Column {!r}".format(col["column_name"])
        for prop, val in col.items():
            if prop not in ignored_properties and val is not None:
                key_target = rels.setdefault(table, {}).setdefault(key, {})
                key_target[prop] = val

    # quick and dirty index comparison
    index_sql = """
        SELECT schemaname || '.' || tablename AS table, indexname AS key, indexdef AS definition
            FROM pg_indexes
            WHERE schemaname = ANY(%s)
                AND NOT (schemaname || '.' || tablename = ANY(%s))
        """
    gather_query_data("Index", rels, cursor, index_sql, [schemas, list(parts)])

    # quick and dirty constraint comparison
    const_sql = """
        SELECT n.nspname || '.' || cl.relname AS table, co.conname AS key, pg_get_constraintdef(co.oid, true) AS definition
            FROM pg_constraint AS co
                JOIN pg_namespace AS n ON (co.connamespace = n.oid)
                JOIN pg_class AS cl ON (co.conrelid = cl.oid)
            WHERE n.nspname = ANY(%s)
                AND NOT (n.nspname || '.' || cl.relname = ANY(%s))
        """
    gather_query_data("Constraint", rels, cursor, const_sql, [schemas, list(parts)])

    # TODO: Sequences
    # TODO: Triggers
    # TODO: Functions

    return {
        "rels": rels,
        "dsn": db.dsn,
        "partitions": sorted(parts),
    }


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


def compare_schema(a_info, b_info, schemas, ignore_partitions):
    yield "--- {}".format(a_info["dsn"])
    yield "+++ {}".format(b_info["dsn"])

    a_rels = a_info["rels"]
    b_rels = b_info["rels"]
    all_rel_names = set(a_rels) | set(b_rels)

    if ignore_partitions:
        all_rel_names -= set(a_info["partitions"])
        all_rel_names -= set(b_info["partitions"])
    if schemas:
        all_rel_names = {
            name
            for name in all_rel_names
            if any(name.startswith(schema + ".") for schema in schemas)
        }

    for rel in sorted(all_rel_names):
        if a_rels.get(rel) == b_rels.get(rel):
            continue

        yield "@@ Relation {}".format(rel)
        if rel not in a_rels:
            yield "+Relation {!r}".format(rel)
            continue
        if rel not in b_rels:
            yield "+Relation {!r}".format(rel)
            continue

        yield from compare_dicts(a_rels[rel], b_rels[rel])


def save_schema(*, conn_str, schemas, ignore_partitions, output_file):
    with psycopg2.connect(conn_str) as db:
        info = gather_data(db, schemas=schemas, ignore_partitions=ignore_partitions)
    with open(output_file, "w") as fp:
        json.dump(info, fp, sort_keys=True, indent=4)
    print("Wrote {!r}".format(output_file))


def compare(*, a_conn_str, b_conn_str, schemas, ignore_partitions):
    count = 0
    infos = []
    for conn_str in [a_conn_str, b_conn_str]:
        if os.path.exists(conn_str):
            # if a file with this name exists consider it a saved schema dump
            with open(conn_str, "r") as fp:
                infos.append(json.load(fp))
        else:
            # otherwise it must be a connection string, gather new data
            with psycopg2.connect(conn_str) as db:
                infos.append(gather_data(db, schemas=schemas, ignore_partitions=ignore_partitions))

    for issue in compare_schema(a_info=infos[0], b_info=infos[1], schemas=schemas, ignore_partitions=ignore_partitions):
        if issue.startswith("@@"):
            count += 1
        print(issue)
    print("Found differences in {} relations".format(count))
    return count


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=False, action="append",
                        help="Schemas to compare (defaults is to compare all schemas)")
    parser.add_argument("--ignore-partitions", action="store_true",
                        help="Ignore all partitions (relations that have parents)")
    parser.add_argument("--save", action="store_true",
                        help="Serialize the schema from first argument's connection "
                             "to file designated by the second argument")
    parser.add_argument("connection_string_1",
                        help="First connection or filename for comparison")
    parser.add_argument("connection_string_2",
                        help="Second connection or filename for comparison; "
                             "output filename when using --save")
    pargs = parser.parse_args(args)

    if pargs.save:
        return save_schema(
            conn_str=pargs.connection_string_1,
            schemas=pargs.schema,
            ignore_partitions=pargs.ignore_partitions,
            output_file=pargs.connection_string_2,
        )
    else:
        diffs = compare(
            a_conn_str=pargs.connection_string_1,
            b_conn_str=pargs.connection_string_2,
            schemas=pargs.schema,
            ignore_partitions=pargs.ignore_partitions,
        )
        return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
