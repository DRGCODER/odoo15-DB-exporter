#!/usr/bin/env python3
"""CLI tool to export Odoo 15 data to CSV files for migration.

Usage:
    python migrate.py --profile maintenance_approval
    python migrate.py --profile maintenance_approval --output-dir ./export
    python migrate.py --list-profiles
"""
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROFILES_PATH = SCRIPT_DIR / "sync_profiles.json"
TABLE_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def validate_identifier(name):
    if not TABLE_RE.match(name):
        print("ERROR: Invalid SQL identifier: %s" % name, file=sys.stderr)
        sys.exit(1)
    return name


def get_connection():
    params = {
        "dbname": os.environ["ODOO15_DB_NAME"],
        "cursor_factory": RealDictCursor,
    }
    host = os.environ.get("ODOO15_DB_HOST")
    if host:
        params["host"] = host
    port = os.environ.get("ODOO15_DB_PORT")
    if port:
        params["port"] = int(port)
    user = os.environ.get("ODOO15_DB_USER")
    if user:
        params["user"] = user
    password = os.environ.get("ODOO15_DB_PASSWORD")
    if password:
        params["password"] = password
    return psycopg2.connect(**params)


def load_profiles():
    if not PROFILES_PATH.exists():
        print("ERROR: sync_profiles.json not found at %s" % PROFILES_PATH, file=sys.stderr)
        sys.exit(1)
    raw = json.loads(PROFILES_PATH.read_text(encoding="utf-8"))
    profiles = {}
    for entry in raw:
        profiles[entry["name"]] = entry
    return profiles


def fetch_all(conn, table):
    table = validate_identifier(table)
    query = f"SELECT * FROM {table}"
    with conn.cursor() as cr:
        cr.execute(query)
        return list(cr.fetchall())


def fetch_children(conn, table, parent_field, parent_ids):
    if not parent_ids:
        return []
    table = validate_identifier(table)
    parent_field = validate_identifier(parent_field)
    query = """
        SELECT *
        FROM {table}
        WHERE {parent_field} = ANY(%(parent_ids)s)
        ORDER BY id
    """.format(table=table, parent_field=parent_field)
    with conn.cursor() as cr:
        cr.execute(query, {"parent_ids": parent_ids})
        return list(cr.fetchall())


def fetch_messages(conn, model_name, record_ids):
    if not record_ids:
        return []
    query = """
        SELECT *
        FROM mail_message
        WHERE model = %(model_name)s
          AND res_id = ANY(%(record_ids)s)
        ORDER BY res_id, date, id
    """
    with conn.cursor() as cr:
        cr.execute(query, {"model_name": model_name, "record_ids": record_ids})
        return list(cr.fetchall())

def serialize_value(val):
    if val is None:
        return ""
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, (list, dict)):
        return json.dumps(val, default=str)
    return val


def write_csv(output_dir, filename, rows):
    if not rows:
        print("  %s: 0 rows (skipped)" % filename)
        return
    filepath = output_dir / filename
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: serialize_value(v) for k, v in row.items()})
    print("  %s: %d rows" % (filename, len(rows)))


def run_export(profile, output_dir):
    profile_name = profile["name"]
    table = profile["table"]
    model = profile["model"]
    children_cfg = profile.get("children") or []

    conn = get_connection()
    try:
        print("Fetching %s records..." % profile_name)
        parent_rows = fetch_all(conn, table)
        if not parent_rows:
            print("No records found in range.")
            return

        parent_ids = [r["id"] for r in parent_rows if r.get("id") is not None]
        print("  Found %d parent records (id %d..%d)" % (
            len(parent_ids), min(parent_ids), max(parent_ids),
        ))

        all_children = {}
        all_model_ids = [(model, parent_ids)]

        for child in children_cfg:
            child_rows = fetch_children(
                conn, child["table"], child["parent_field"], parent_ids,
            )
            all_children[child["key"]] = child_rows
            child_ids = [r["id"] for r in child_rows if r.get("id") is not None]
            if child_ids:
                all_model_ids.append((child["model"], child_ids))
            print("  Found %d child '%s' records" % (len(child_rows), child["key"]))

        all_messages = []
        for model_name, ids in all_model_ids:
            msgs = fetch_messages(conn, model_name, ids)
            all_messages.extend(msgs)
            print("  [%s] %d messages" % (model_name, len(msgs)))

    finally:
        conn.close()

    print("\nWriting CSV files to %s/" % output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_csv(output_dir, "%s.csv" % profile_name, parent_rows)

    for child in children_cfg:
        key = child["key"]
        write_csv(output_dir, "%s_%s.csv" % (profile_name, key), all_children.get(key, []))

    write_csv(output_dir, "%s_messages.csv" % profile_name, all_messages)

    print("\nDone.")


def main():
    load_dotenv(SCRIPT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="Export Odoo 15 data to CSV files for migration.",
    )
    parser.add_argument("--profile", help="Profile name from sync_profiles.json")
    parser.add_argument("--output-dir", default="./export", help="Output directory (default: ./export)")
    parser.add_argument("--list-profiles", action="store_true", help="List available profiles and exit")
    args = parser.parse_args()

    profiles = load_profiles()

    if args.list_profiles:
        print("Available profiles:")
        for name, p in profiles.items():
            children_desc = ", ".join(c["key"] for c in p.get("children", []))
            print("  %-25s table=%-45s children=[%s]" % (name, p["table"], children_desc))
        return

    if not args.profile:
        parser.error("--profile is required (use --list-profiles to see available)")

    if args.profile not in profiles:
        print("ERROR: Unknown profile '%s'. Available: %s" % (
            args.profile, ", ".join(profiles.keys()),
        ), file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output_dir)
    run_export(profiles[args.profile], output_dir)


if __name__ == "__main__":
    main()
