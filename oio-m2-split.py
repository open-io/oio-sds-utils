#!/usr/bin/env python

from os import makedirs
from os.path import dirname, exists
from sys import argv
from sqlite3 import connect
from itertools import product
from traceback import print_exc
from sys import stderr
import argparse

from oio.common.utils import cid_from_name


hexa = "0123456789abcdef"

nb_xdigits = 2
flag_prune = False
flag_vacuum = False


def prefixes():
    plop = list()
    for i in range(nb_xdigits):
        plop.append(hexa)
    for prefix in product(*plop):
        yield ''.join(prefix)


def whois(db):
    uname, aname = None, None
    for row in db.execute("SELECT v FROM admin WHERE k = 'sys.user.name'"):
        uname = str(row[0])
    for row in db.execute("SELECT v FROM admin WHERE k = 'sys.account'"):
        aname = str(row[0])
    return aname, uname


def compute_new_cname(acct, cname, prefix):
    return acct, cname + '/' + prefix


def prune_database(db, cname, cid, prefix):
    tnx = db.cursor()
    tnx.execute("UPDATE admin SET v = ? WHERE k = 'sys.user.name'", (cname, ))
    tnx.execute("UPDATE admin SET v = ? WHERE k = 'sys.name'", (cid + '.1', ))
    # TODO(jfs): Drop the FROZEN flag
    if flag_prune:
        tnx.execute("DELETE FROM aliases WHERE alias NOT LIKE 'cloud_images/{0}%'".format(prefix))
        tnx.execute("DELETE FROM contents WHERE id NOT IN (SELECT DISTINCT content FROM aliases)")
        tnx.execute("DELETE FROM properties WHERE alias NOT IN (SELECT alias FROM aliases)")
        tnx.execute("DELETE FROM chunks WHERE content NOT IN (SELECT id FROM contents)")
    db.commit()
    if flag_vacuum:
        db.execute("VACUUM")


def main():
    parser = argparse.ArgumentParser(description='Split a meta2 container')
    parser.add_argument('--prune', dest='prune', default=False, action='store_true',
                        help='Remove the contents not belonging to this container')
    parser.add_argument('--vacuum', dest='vacuum', default=False, action='store_true',
                        help='Vacuum the container at the end of the procedure')
    parser.add_argument('--target', dest='target', type=str, default='/tmp/wrk',
                        help='target directory for all the copies')
    parser.add_argument('--digits', dest='xdigits', type=int, default=2,
                        help='How many xdigits should be considered in the sharding')
    parser.add_argument('container', metavar='<container>', type=str,
                        help='The path to a container')
    args = parser.parse_args()

    basedir = args.target
    path = args.container
    global flag_vacuum
    global flag_prune
    global nb_xdigits
    flag_vacuum = args.vacuum
    flag_prune = args.prune
    nb_xdigits = args.xdigits

    if not exists(path):
        raise Exception("DB not found")

    acct, cname = None, None
    with connect(path) as db:
        acct, cname = whois(db)

    if not acct or not cname:
        raise Exception("Container unknown")
    cid = cid_from_name(acct, cname)
    print "#<", path, acct, cname, cid
    if cname != 'cloud_images':
        raise Exception("Container is not 'cloud_images'")

    for prefix in prefixes():
        new_acct, new_cname = compute_new_cname(acct, cname, prefix)
        new_cid = cid_from_name(new_acct, new_cname)
        new_path = basedir + '/' + new_cid[0:3] + '/' + new_cid + '.1.meta2'
        print "#+", new_path, new_acct, new_cname, new_cid

        try:
            makedirs(dirname(new_path))
        except OSError:
            pass

        try:
            #from shutil import copyfile
            #copyfile(path, new_path)
            from subprocess import check_call
            check_call(["/bin/cp", "-p", path, new_path])
            with connect(new_path) as db:
                prune_database(db, new_cname, new_cid, prefix)
        except Exception as e:
            print_exc(file=stderr)


if __name__ == '__main__':
    main()
