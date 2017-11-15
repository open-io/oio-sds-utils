#!/usr/bin/env python

from os import makedirs
from os.path import dirname, exists
from sqlite3 import connect
from itertools import product
from sys import stderr
import logging

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
    return acct, ''.join([cname, prefix])


def prune_database(db, cname, cid, prefix):
    db.execute("PRAGMA journal_mode = MEMORY")
    db.execute("PRAGMA foreign_keys = FALSE")
    db.execute("PRAGMA synchronous = OFF")
    tnx = db.cursor()
    tnx.execute("UPDATE admin SET v = ? WHERE k = 'sys.user.name'", (cname, ))
    tnx.execute("UPDATE admin SET v = ? WHERE k = 'sys.name'", (cid + '.1', ))
    if flag_prune:
        tnx.execute("DELETE FROM aliases "
                    "WHERE alias NOT LIKE 'cloud_images/{0}%'".format(prefix))
        tnx.execute("DELETE FROM contents "
                    "WHERE id NOT IN (SELECT DISTINCT content FROM aliases)")
        tnx.execute("DELETE FROM properties "
                    "WHERE alias NOT IN (SELECT alias FROM aliases)")
        tnx.execute("DELETE FROM chunks "
                    "WHERE content NOT IN (SELECT id FROM contents)")
    tnx.execute("UPDATE admin SET v = 0 WHERE k = 'sys.status'")
    tnx.execute("DELETE FROM admin WHERE k = 'sys.peers'")
    tnx.execute("UPDATE admin SET v = (SELECT COUNT(*) FROM aliases) WHERE k = 'sys.m2.objects'")
    tnx.execute("UPDATE admin SET v = (SELECT SUM(c.size) FROM contents c, aliases a WHERE c.id = a.content) WHERE k = 'sys.m2.usage'")
    db.commit()
    if flag_vacuum:
        db.execute("VACUUM")

def sharded_container(basedir, acct, cname, path, as_prefix=""):
    for prefix in prefixes():
        new_acct, new_cname = compute_new_cname(acct, cname, prefix)
        new_cid = cid_from_name(new_acct, new_cname)
        new_path = basedir + '/' + new_cid[0:3] + '/' + new_cid + '.1.meta2'
        logging.debug("%s %s %s %s", new_path, new_acct, new_cname, new_cid)

        try:
            makedirs(dirname(new_path))
        except OSError:
            pass

        try:
            from subprocess import check_call
            check_call(["/bin/cp", "-p", path, new_path])
            with connect(new_path) as db:
                prune_database(db, new_cname, new_cid,
                               ''.join([as_prefix, prefix]))
            print new_acct, new_cname, new_cid
        except Exception:
            from traceback import print_exc
            print_exc(file=stderr)


def main():
    # Parse the CLI args
    import argparse
    parser = argparse.ArgumentParser(description='Split a meta2 container')
    parser.add_argument('--verbose', '-v', dest='verbose',
                        default=False, action='store_true',
                        help='Increase the verbosity of the process')
    parser.add_argument('--prune', dest='prune',
                        default=False, action='store_true',
                        help='Remove contents')
    parser.add_argument('--vacuum', dest='vacuum',
                        default=False, action='store_true',
                        help='VACUUM the DB file for dirty pages')
    parser.add_argument('--target', dest='target',
                        type=str, default='/tmp/wrk',
                        help='target directory for all the copies')
    parser.add_argument('--digits', dest='xdigits',
                        type=int, default=2,
                        help='Set the sharding width (in number of chars)')
    parser.add_argument('--already-sharded-digits', dest ='already_sharded_xdigits',
                        type=int ,default=0,
                        help='Set the sharding width of an already sharded container')
    parser.add_argument('container', metavar='<container>',
                        type=str,
                        help='The path to a container')
    args = parser.parse_args()

    # Patch the core execution flags
    global flag_vacuum
    global flag_prune
    global nb_xdigits
    global nb_as_xdigits
    flag_vacuum = args.vacuum
    flag_prune = args.prune
    nb_xdigits = args.xdigits
    nb_as_xdigits = args.already_sharded_xdigits
    # Configure the logging
    if args.verbose:
        logging.basicConfig(
            format='%(asctime)s %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=logging.DEBUG)
    else:
        logging.basicConfig(
            format='%(asctime)s %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=logging.INFO)

    # Start working on the DB file
    basedir = args.target
    path = args.container
    if not exists(path):
        raise Exception("DB not found")

    acct, cname = None, None
    with connect(path) as db:
        acct, cname = whois(db)

    if not acct or not cname:
        raise Exception("Container unknown")
    cid = cid_from_name(acct, cname)
    logging.debug("%s %s %s %s", path, acct, cname, cid)
    if cname.find('cloud_images') > 0:
        raise Exception("Container is not 'cloud_images'")

    if nb_as_xdigits == 0:
        sharded_container(basedir, acct, cname, path)
        return
    else:
        as_pre = cname[-nb_as_xdigits:]
        logging.debug("%s %s %s %s", path, acct, cname, cid)
        sharded_container(basedir, acct, cname, path, as_prefix=as_pre)

if __name__ == '__main__':
    main()
