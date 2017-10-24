#!/usr/bin/env python

from os import makedirs
from os.path import dirname
from sys import argv
from sqlite3 import connect
from itertools import product
from traceback import print_exc
from sys import stderr

from oio.common.utils import cid_from_name


nb_xdigits = 2
# The case matters
hexa = "012345678abcdef"


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
    tnx.execute("DELETE FROM aliases WHERE alias NOT LIKE 'cloud_images/{0}%'".format(prefix))
    tnx.execute("DELETE FROM contents WHERE id NOT IN (SELECT DISTINCT content FROM aliases)")
    tnx.execute("DELETE FROM properties WHERE alias NOT IN (SELECT alias FROM aliases)")
    tnx.execute("DELETE FROM chunks WHERE content NOT IN (SELECT id FROM contents)")
    db.commit()
    db.execute("VACUUM")


def main():
    basedir = argv[1]
    path = argv[2]

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
