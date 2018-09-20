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
import argparse
import math
import os

from oio.api.object_storage import ObjectStorageApi


ACCOUNT = None
PROXY = None


def container_hierarchy(bucket, path):
    if not path:
        return bucket
    ch = '%2F'.join(path.rstrip('/').split('/'))
    return bucket + '%2F' + ch


def get_list(bucket):
    items = PROXY.object_list(ACCOUNT, bucket)
    todo = []
    _size = 0
    _files = 0
    for entry in items['objects']:
        if entry['name'].endswith('/'):
            todo.append(container_hierarchy(bucket, entry['name']))
        else:
            _size += entry['size']
            _files += 1

    return _files, _size, todo


def options():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=os.getenv("OIO_ACCOUNT", "demo"))
    parser.add_argument("--namespace", default=os.getenv("OIO_NS", "OPENIO"))
    parser.add_argument("--human", "-H", action="store_true", default=False)
    parser.add_argument("path", help="bucket/path1/path2")

    return parser.parse_args()


def show(size, human=False):
    if not human:
        return "%10d" % size

    if size == 0:
        return "%10s" % "0B"

    size_name = ("iB", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    return "%7s%s" % (s, size_name[i])


def full_list(**kwargs):
        listing = PROXY.container_list(
            ACCOUNT, **kwargs)
        for element in listing:
            yield element

        while listing:
            kwargs['marker'] = listing[-1][0]
            listing = PROXY.container_list(
                ACCOUNT, **kwargs)
            if listing:
                for element in listing:
                    yield element


def main():
    args = options()

    global ACCOUNT, PROXY
    ACCOUNT = args.account
    PROXY = ObjectStorageApi("OPENIO")

    args.path = args.path.rstrip('/')
    if '/' in args.path:
        bucket, path = args.path.split('/', 1)
    else:
        bucket = args.path
        path = ""

    items = [container_hierarchy(bucket, path)]
    files = 0
    size = 0

    SUM = {}

    files = 0
    size = 0
    _bucket = container_hierarchy(bucket, path)
    for entry in full_list(prefix=container_hierarchy(bucket, path)):
        name, _files, _size, _ = entry
        if name != _bucket and not name.startswith(_bucket + '%2F'):
            continue
        size += _size
        files += _files

        items = name.split('%2F')
        while items:
            _name = '/'.join(items)
            if not _name.startswith(args.path):
                break
            if _name in SUM:
                SUM[_name] += _size
            else:
                SUM[_name] = _size
            items.pop()

    view = [(v, k) for k, v in SUM.items()]
    view.sort()
    for v, k in view:
        print("%s  %s" % (show(v, args.human), k))

    print("found %d files, %s bytes" % (files, size))


if __name__ == "__main__":
    main()
