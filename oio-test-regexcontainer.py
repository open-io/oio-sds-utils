#!/usr/bin/python2
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

import sys
from oio.common.autocontainer import NoMatchFound, RegexContainerBuilder

# If no object is provided on command line, match these object names.
OBJ_NAMES = [
    'notprefix/sub/0a1b2c3d-object',
    'notprefix/sub/0a1b2c-object',
    'prefix/sub/0a1b2c3d-object',
    'prefix/0a1b2c3d-object',
    '0a1b2c3d-object',
]

# You can edit these rules to add yours, or comment out those you don't want.
RULES = [
    # Capture 3 groups of digits between slashes.
    r'/(\d+)/(\d+)/(\d+)/',
    # Capture a prefix followed by a slash and 2 hexdigits.
    r'^(prefix)/([0-9a-f][0-9a-f])',
    # Match anything up to last forward slash, plus any first two hexadecimal
    # digits in a URL that has a series of 8 (opportunistic sharding).
    r'^(.+)/.*?([0-9A-Fa-f]{2})(?=[0-9A-Fa-f]{6})',
    # Capture anything up to the first forward slash.
    r'^/?([^/]+)',
]


def describe_builder(builder):
    """
    Print a description of the rules.
    """
    for num, rule in enumerate(builder.patterns):
        print "# Rule %d (%d groups): %s" % (num, rule.groups, rule.pattern)


def match_rules(builder, obj_names):
    """
    Build a container name with `builder` for each object.
    """
    print "# Input -> container name"
    for obj in obj_names:
        if obj[0] == '/':
            obj = obj[1:]
        try:
            ct = builder(obj)
            print '%s -> %s' % (obj, ct)
        except NoMatchFound as err:
            print '%s -> %s' % (obj, err)


if __name__ == '__main__':
    BUILDER = RegexContainerBuilder(RULES)
    describe_builder(BUILDER)
    if len(sys.argv) > 1:
        match_rules(BUILDER, sys.argv[1:])
    else:
        match_rules(BUILDER, OBJ_NAMES)
