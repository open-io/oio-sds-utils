#!/usr/bin/python
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

from oio.common import exceptions
from oio.common.green import time
from oio.common.utils import request_id
from oio.rdir.client import RdirClient


USAGE = """Copy rdir records.

usage: %s <RAWX> from|to <RDIR>

from: copy records from <RDIR> to the current rdir assigned to <RAWX>
to: copy records from the current rdir assigned to <RAWX> to <RDIR>

Namespace name is read from OIO_NS environment variable."""

def copy_rdir(src_vol, rdir_in, rdir_out):
    """
    Copy all rdir records related to src_vol from rdir_in to rdir_out.
    """
    n_chunks = 0
    n_errors = 0
    start = time.time()
    reqid = 'rdir-copy-%s' % request_id()[:-10]
    headers_in = {'X-oio-req-id': reqid}
    headers_out = dict()
    for record in rdir_in.chunk_fetch(src_vol, headers=headers_in):
        cid, content, chunk, data = record
        headers_out['X-oio-req-id'] = '%s-%d' % (reqid, n_chunks)
        n_chunks += 1
        print("%s|%s|%s" % (cid, content, chunk), end=' ')
        try:
            rdir_out.chunk_push(src_vol, cid, content, chunk,
                                headers=headers_out, **data)
            print("indexed in %s" % rdir_out._get_rdir_addr(src_vol, reqid))
        except exceptions.OioException as err:
            n_errors += 1
            print(err)
    duration = time.time() - start
    print("%d chunks records read, %d errors, %fs" % (
        n_chunks, n_errors, duration))
    print("%.1f chunks/s" % (n_chunks / duration))

def copy_rdir_from(ns, src_vol, src_rdir):
    """
    Fill the current rdir service with records from an old one.
    Do this after unlink/bootstrap.
    """
    rdir_in = RdirClient({'namespace': ns})
    rdir_in._get_rdir_addr = lambda x, y: src_rdir
    rdir_out = RdirClient({'namespace': ns}, pool_manager=rdir_in.pool_manager)
    return copy_rdir(src_vol, rdir_in, rdir_out)

def copy_rdir_to(ns, src_vol, dst_rdir):
    """
    Fill the future rdir service with records from the current one.
    Do this before unlink/bootstrap.
    """
    rdir_in = RdirClient({'namespace': ns})
    rdir_out = RdirClient({'namespace': ns}, pool_manager=rdir_in.pool_manager)
    rdir_out._get_rdir_addr = lambda x, y: dst_rdir
    return copy_rdir(src_vol, rdir_in, rdir_out)


if __name__ == '__main__':
    NS = os.getenv('OIO_NS')
    if len(sys.argv) < 4:
        print("missing arguments")
        print()
        print(USAGE % sys.argv[0])
        sys.exit(1)
    elif not NS:
        print("missing namespace")
        print()
        print(USAGE % sys.argv[0])
        sys.exit(2)
    SRC_VOL = sys.argv[1]
    VERB = sys.argv[2]
    RDIR = sys.argv[3]
    if VERB == 'to':
        copy_rdir_to(NS, SRC_VOL, RDIR)
    elif VERB == 'from':
        copy_rdir_from(NS, SRC_VOL, RDIR)
    else:
        print("invalid verb: %s" % VERB)
        print()
        print(USAGE % sys.argv[0])
        sys.exit(3)
