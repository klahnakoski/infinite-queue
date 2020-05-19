# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_files import File
from mo_kwargs import override

TABLE_PREFIX = "infinite_queues"
VERSION_TABLE = TABLE_PREFIX + "_version"
QUEUE = TABLE_PREFIX + "_queue"
SUBSCRIBER = TABLE_PREFIX + "_subscriber"
MESSAGES = TABLE_PREFIX + "_messages"
UNCONFIRMED = TABLE_PREFIX + "_unconfirmed"


class DirectoryBacking:
    @override
    def __init__(self, directory):
        self.dir = File(directory)

    def write_lines(self, key, lines):
        (self.dir / key).set_extension("json").write_lines(lines)

    def url(self, key):
        return "file:///" + (self.dir / key).set_extension("json").abspath

