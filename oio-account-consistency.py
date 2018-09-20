#!/usr/bin/env python
# Copyright (C) 2018 OpenIO SAS, as part of OpenIO SDS
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
from time import time
import argparse
import sys
import os
import json

from oio.directory.client import DirectoryClient
from oio.common.http import get_pool_manager
from oio.account.backend import AccountBackend
from oio.common.exceptions import NotFound

ACCOUNT = "murlock"
NAMESPACE = "OPENIO"
HOST = '127.0.0.1:6035'


def full_list(backend, **kwargs):
        listing = backend.list_containers(ACCOUNT, **kwargs)
        for element in listing:
            yield element

        while listing:
            kwargs['marker'] = listing[-1][0]
            listing = backend.list_containers(ACCOUNT, **kwargs)
            if listing:
                for element in listing:
                    yield element


def options(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=os.getenv("OIO_ACCOUNT", "demo"))
    parser.add_argument("--namespace", default=os.getenv("OIO_NS", "OPENIO"))
    parser.add_argument("--verbose", default=False, action="store_true")
    parser.add_argument("--dry-run", default=False, action="store_true")
    parser.add_argument("--prefix", help="prefix of containers of check")
    parser.add_argument("--redis-sentinel-hosts", dest="sentinel_hosts",
                        default=None, help="sentinel hosts")
    parser.add_argument("--redis-sentinel-master-name",
                        dest="sentinel_master_name", default='oio',
                        help="sentinel name")
    parser.add_argument("--redis-host", dest="sentinel_master_name",
                        default='127.0.0.1', help="redis single host")
    parser.add_argument("--redis-port", default=6379, help="redis single host")
    parser.add_argument("host", help="IP:PORT of Account service")

    return parser.parse_args()


def run(args):
    pool = get_pool_manager()

    v = vars(args)

    dirclient = DirectoryClient(v)
    backend = AccountBackend(v)

    for entry, _, _, partial in full_list(backend, prefix=args.prefix):
        if partial:
            if args.verbose:
                print(":%s: partial, skip" % entry)
            continue
        try:
            dirclient.show(account=ACCOUNT, reference=entry)
            if args.verbose:
                print("%s: OK" % entry)
            continue
        except NotFound:
            pass
        except Exception as exc:
            print("Exception not managed for %s: %s" % (entry, str(exc)))
            continue
        print("%s: entry not found" % entry)
        if args.dry_run:
            continue

        data = {"dtime": time(), "name": entry}
        # post event to Account service
        res = pool.request(
            'POST',
            HOST + '/v1.0/account/container/update?id=%s' % ACCOUNT,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(data))
        if res.status / 100 != 2:
            print(res.status)


if __name__ == "__main__":
    args = options(sys.argv)
    ACCOUNT = args.account
    NAMESPACE = args.namespace
    HOST = args.host
    run(args)
