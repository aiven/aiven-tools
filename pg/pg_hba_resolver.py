#!/usr/bin/python
"""
pg_hba_resolver - read pg_hba.conf and resolve DNS names to addresses

Copyright (C) 2016, https://aiven.io/
This file is under the Apache License, Version 2.0.
See http://www.apache.org/licenses/LICENSE-2.0 for details.

Read pg_hba.conf and look for comment lines ending with a '# RESOLVE' tag:

Hostnames on such lines are looked up and pg_hba.conf is updated with
uncommented versions of such lines with the hostname replaced by the
resolved address.

Existing entries for the same names are removed if the name no longer
resolves to them.  Postgres is reloaded by sending SIGHUP to the postmaster
if there are any changes.
"""

import os
import re
import signal
import socket
import sys


def update_hba(hba_filename, pid_filename=None):
    with open(hba_filename, "r") as fp:
        orig_hba = fp.read().splitlines()

    template_re = re.compile(r"^\s*#\s*(host[nosl]*\s+\S+\s+\S+\s+(\S+)\s+.*?)\s*#\s*RESOLVE\s*$")
    last_host_comment = None
    addrmap = {}
    new_hba = []

    for line in orig_hba:
        if last_host_comment and line.endswith(last_host_comment):
            continue  # Drop all previous entries for this host

        # All other lines are included as-is in the config
        new_hba.append(line)

        # If the line matches our template for resolving, look up the host and add all addresses
        match = template_re.match(line)
        if not match:
            continue

        entry = match.group(1)
        hostname = match.group(2)
        if hostname not in addrmap:
            try:
                addrs = socket.getaddrinfo(hostname, 5432, socket.AF_INET, 0, socket.IPPROTO_TCP)
                addrmap[hostname] = sorted(res[4][0] for res in addrs)
            except socket.gaierror:
                print("Unable to resolve {!r}".format(hostname))
                addrmap[hostname] = []

        last_host_comment = " # RESOLVED: {}".format(hostname)
        for addr in addrmap[hostname]:
            new_hba.append(entry.replace(hostname, addr + "/32") + last_host_comment)

    if not addrmap:
        name_list = "no names"
    else:
        name_list = ", ".join("{!r}".format(name) for name in sorted(addrmap))

    if new_hba == orig_hba:
        print("Looked up {}: no changes to {!r} required".format(name_list, hba_filename))
        return

    print("Looked up {}: updating {}".format(name_list, hba_filename))
    for line in sorted(set(orig_hba) - set(new_hba)):
        print("-{}".format(line))
    for line in sorted(set(new_hba) - set(orig_hba)):
        print("+{}".format(line))

    with open(hba_filename, "w") as fp:
        fp.write("\n".join(new_hba))
        fp.write("\n")

    if not pid_filename:
        pid_filename = os.path.join(os.path.dirname(hba_filename), "postmaster.pid")

    if not os.path.exists(pid_filename):
        print("No {!r} found, not reloading postmaster".format(pid_filename))
        return

    with open(pid_filename, "r") as fp:
        pid = int(fp.read().splitlines()[0])
    print("Sending SIGHUP to postmaster process {!r}".format(pid))
    os.kill(pid, signal.SIGHUP)


def main(args):
    update_hba(hba_filename=args[0], pid_filename=args[1] if len(args) > 1 else None)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
