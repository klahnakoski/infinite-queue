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
from mo_times import Date

TABLE_PREFIX = "irq"
VERSION_TABLE = "version"
QUEUE = "queue"
BLOCKS = "blocks"
SUBSCRIBER = "subscriber"
MESSAGES = "messages"
UNCONFIRMED = "unconfirmed"


class DirectoryBacking:
    @override
    def __init__(self, directory):
        self.dir = File(directory)

    def write_lines(self, key, lines):
        (self.dir / key).set_extension("json").write_lines(lines)

    def url(self, key):
        return "file:///" + (self.dir / key).set_extension("json").abspath

    def read_lines(self, key):
        return (self.dir / key).set_extension("json").read_lines()


def _path(timestamp):
    return Date(timestamp).format("%Y/%m/%d")

