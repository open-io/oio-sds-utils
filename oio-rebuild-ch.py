#!/usr/bin/env python
# Copyright (C) 2019 OpenIO SAS, as part of OpenIO SDS
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

import os
import sys

# from six.moves.urllib.parse import unquote
from urllib import unquote

from oio import ObjectStorageApi
from oio.common.json import json
from oio.common.utils import depaginate


def bucket_shards_key(account, bucket):
    return "CS:%s:%s:cnt" % (account, bucket)


def redis_insert_shard(account, bucket, shard):
    return 'ZADD "%s" 1 "%s"\n' % (bucket_shards_key(account, bucket), shard)


def crawl_account_containers(api, account,
                             cmd_out=None,
                             compare_to=None):
    """
    Crawl the list of containers from the account, and build the dict of
    shards of all buckets.

    :param cmd_out: open file where to write the Redis script to rebuild
        the container_hierarchy DB
    :param print_redis_commands: print Redis commands to reinsert shards in DB
    """
    for ct in depaginate(api.container_list,
                         marker_key=lambda x: x[-1][0],
                         account=account,
                         attempts=3):
        cname = ct[0].encode('utf-8')
        if '%2F' not in cname:
            print('Found bucket %s (ctime: %s)' % (cname, ct[4]))
            continue
        bucket, shard = cname.split('%2F', 1)
        shard = unquote(shard) + '/'
        if not ct[1]:
            print('Found EMTPY shard "%s" of bucket %s from account %s' % (
                  shard, bucket, account))
            continue
        print('Found shard "%s" of bucket %s from account %s' % (
              shard, bucket, account), end='')
        if compare_to is not None:
            key = bucket_shards_key(account, bucket)
            if shard not in compare_to.get(key, {}):
                print(', not found in DB dump', end='')
                if cmd_out:
                    cmd_out.write(redis_insert_shard(account, bucket, shard))
        elif cmd_out:
            cmd_out.write(redis_insert_shard(account, bucket, shard))
        print()


def crawl_all_accounts(api, cmd_out=None, compare_to=None):
    """
    Apply `crawl_account_containers` on all accounts of the namespace.
    """
    all_accounts = api.account_list()
    for acct in all_accounts:
        try:
            crawl_account_containers(
                api, acct, cmd_out=cmd_out, compare_to=compare_to)
        except Exception as exc:
            print('Failed to crawl containers of %s: %s' % (acct, exc))


def usage(cmd):
    print('Usage: %s [redis_script_out [json_dump]]' % cmd, file=sys.stderr)
    print('')
    print('  redis_script_out: where to write the Redis script to rebuild')
    print('                    the container_hierarchy database.')
    print('')
    print('  json_dump:        JSON dump of the Redis database.')
    print('                    If specified, write the script only for')
    print('                    missing entries.')


if __name__ == '__main__':
    NS = os.getenv('OIO_NS', 'OPENIO')
    ACCT = os.getenv('OIO_ACCOUNT')
    API = ObjectStorageApi(NS)

    CMD_OUT = None
    DUMP = None
    if len(sys.argv) > 1:
        if sys.argv[1] in ('-h', '--help'):
            usage(sys.argv[0])
            sys.exit(1)
        SCRIPT_PATH = sys.argv[1]
        CMD_OUT = open(SCRIPT_PATH, 'w')
        if len(sys.argv) > 2:
            DUMP_PATH = sys.argv[2]
            with open(DUMP_PATH, 'r') as dump_file:
                DUMP = json.load(dump_file)
                if isinstance(DUMP, list):
                    DUMP = DUMP[0]

    if ACCT:
        crawl_account_containers(API, ACCT, cmd_out=CMD_OUT, compare_to=DUMP)
    else:
        crawl_all_accounts(API, cmd_out=CMD_OUT, compare_to=DUMP)

    if CMD_OUT:
        CMD_OUT.close()
