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
import threading
import eventlet
from eventlet import Queue
import sys
import time
from oio.api.object_storage import ObjectStorageApi

eventlet.monkey_patch()

NS = None
ACCOUNT = None
PROXY = None
VERBOSE = False
TIMEOUT = 5
COUNTERS = None
ELECTIONS = None


class AtomicInteger():
    def __init__(self):
        self._files = 0
        self._size = 0
        self._lock = threading.Lock()
        self._total_files = 0
        self._total_size = 0
        self._start = time.time()

    def add(self, files, size):
        with self._lock:
            self._files += files
            self._size += size

            self._total_files += files
            self._total_size += size

    def reset(self):
        with self._lock:
            val = (self._files, self._size)
            self._files = 0
            self._size = 0
            return val

    def total(self):
        with self._lock:
            return (self._total_files, self._total_size)

    def time(self):
        return time.time() - self._start


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


def worker_objects():
    proxy = ObjectStorageApi(NS)
    while True:
        try:
            name = QUEUE.get(timeout=TIMEOUT)
        except eventlet.queue.Empty:
            if VERBOSE:
                print("Leaving worker")
            break

        while True:
            try:
                items = proxy.object_list(ACCOUNT, name)
                objs = [_item['name'] for _item in items['objects']]
                size = sum([_item['size'] for _item in items['objects']])
                if len(objs) == 0:
                    break
                if VERBOSE:
                    print("Deleting", len(objs), "objects")
                proxy.object_delete_many(ACCOUNT, name, objs=objs)
                COUNTERS.add(len(objs), size)
                break
            except Exception as ex:
                if "Election failed" in str(ex):
                    # wait default Election wait delay
                    ELECTIONS.add(1, 0)
                    time.sleep(20)
                    continue
                print("Objs %s: %s" % (name, str(ex)), file=sys.stderr)
                break

        QUEUE.task_done()


def worker_container():
    proxy = ObjectStorageApi(NS)
    while True:
        try:
            name = QUEUE.get(timeout=TIMEOUT)
        except eventlet.queue.Empty:
            break

        while True:
            if VERBOSE:
                print("Deleting", name)
            try:
                proxy.container_delete(ACCOUNT, name)
                COUNTERS.add(1, 0)
                break
            except Exception as ex:
                if "Election failed" in str(ex):
                    # wait default Election wait delay
                    ELECTIONS.add(1, 0)
                    time.sleep(20)
                    continue
                print("Container %s: %s" % (name, str(ex)), file=sys.stderr)
                break

        QUEUE.task_done()


def container_hierarchy(bucket, path):
    if not path:
        return bucket
    ch = '%2F'.join(path.rstrip('/').split('/'))
    return bucket + '%2F' + ch


def options():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=os.getenv("OIO_ACCOUNT", "demo"))
    parser.add_argument("--namespace", default=os.getenv("OIO_NS", "OPENIO"))
    parser.add_argument("--max-worker", default=20, type=int)
    parser.add_argument("--verbose", default=False, action="store_true")
    parser.add_argument("--timeout", default=5, type=int)
    parser.add_argument("--report", default=60, type=int,
                        help="Report progress every X seconds")
    parser.add_argument("path", nargs='+', help="bucket/path1/path2")

    return parser.parse_args()


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

    global ACCOUNT, PROXY, QUEUE, NS, VERBOSE, TIMEOUT
    global COUNTERS, ELECTIONS
    ACCOUNT = args.account
    NS = args.namespace
    VERBOSE = args.verbose
    TIMEOUT = args.timeout
    PROXY = ObjectStorageApi(NS)
    ELECTIONS = AtomicInteger()

    num_worker_threads = int(args.max_worker)
    print("Using %d workers" % num_worker_threads)

    total_objects = {'size': 0,
                     'files': 0,
                     'elapsed': 0}
    total_containers = {'size': 0,
                        'files': 0,
                        'elapsed': 0}

    for path in args.path:
        path = path.rstrip('/')
        if '/' in path:
            bucket, path = path.split('/', 1)
        else:
            bucket = path
            path = ""

        containers = []

        QUEUE = Queue()
        pool = eventlet.GreenPool(num_worker_threads)

        for i in range(num_worker_threads):
            pool.spawn(worker_objects)

        COUNTERS = AtomicInteger()
        _bucket = container_hierarchy(bucket, path)
        # we don't use placeholders, we use prefix path as prefix
        for entry in full_list(prefix=container_hierarchy(bucket, path)):
            name, _files, _size, _ = entry
            if name != _bucket and not name.startswith(_bucket + '%2F'):
                continue

            if _files:
                QUEUE.put(name)

            containers.append(name)

        # we have to wait all objects
        print("Waiting flush of objects")

        report = args.report

        while not QUEUE.empty():
            ts = time.time()
            while time.time() - ts < report and not QUEUE.empty():
                time.sleep(1)
            diff = time.time() - ts
            val = COUNTERS.reset()
            elections = ELECTIONS.reset()
            print("Objects: %5.2f / Size: %5.2f" % (
                  val[0] / diff, val[1] / diff),
                  "Elections failed: %5.2f/s total: %d" % (
                  elections[0] / diff, ELECTIONS.total()[0]
                  ), " " * 20,
                  end='\r')
            sys.stdout.flush()

        print("Waiting end of workers")
        QUEUE.join()

        val = COUNTERS.total()
        total_objects['files'] += val[0]
        total_objects['size'] += val[1]
        total_objects['elapsed'] += COUNTERS.time()

        COUNTERS = AtomicInteger()

        QUEUE = Queue()
        for i in range(num_worker_threads):
            pool.spawn(worker_container)

        print("We have to delete", len(containers), "containers")

        for container in containers:
            QUEUE.put(container)

        while not QUEUE.empty():
            ts = time.time()
            while time.time() - ts < report and not QUEUE.empty():
                time.sleep(1)
            diff = time.time() - ts
            val = COUNTERS.reset()
            elections = ELECTIONS.reset()
            print("Containers: %5.2f" % (val[0] / diff),
                  "Elections failed: %5.2f/s total: %d" % (
                  elections[0] / diff, ELECTIONS.total()[0]
                  ), " " * 20,
            end='\r')
            sys.stdout.flush()

        QUEUE.join()
        val = COUNTERS.total()
        total_containers['files'] += val[0]
        total_containers['size'] += val[1]
        total_containers['elapsed'] += COUNTERS.time()

    print("""
Objects:
    - ran during {o[elapsed]:5.2f}
    - {o[files]} objects removed (size {size})
    - {o_file_avg:5.2f} objects/s ({o_size_avg} avg. size/s)
""".format(o=total_objects,
           size=show(total_objects['size'], True),
           o_file_avg=total_objects['files']/total_objects['elapsed'],
           o_size_avg=show(total_objects['size']/total_objects['elapsed'], True)))

    print("""
Containers:
    - ran during {o[elapsed]:5.2f}
    - {o[files]} containers
    - {o_file_avg:5.2f} containers/s
""".format(o=total_containers,
           o_file_avg=total_containers['files']/total_containers['elapsed']))

    print("Elections failed: %d" % ELECTIONS.total()[0])

if __name__ == "__main__":
    main()
