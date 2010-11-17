#!/usr/bin/env python

import contextlib
import gzip
import time
from optparse import OptionParser

from analytics import Analytics
from analytics.loggers import RotatingFileLogger
from analytics.loggers.file import HiveFormatter
from analytics.parsers import PARSERS

class ParseLog(object):
    def __init__(self, event, filename, outpath, parser, forever=False):
        self.event = event
        self.filename = filename
        self.formatter = HiveFormatter()
        self.logger = RotatingFileLogger(outpath, self.formatter)
        self.analytics = Analytics(self.logger)
        self.parser = parser
        self.forever = forever
    
    def run(self):
        if self.filename.endswith('.gz'):
            fp = gzip.GzipFile(self.filename, "r")
        else:
            fp = open(self.filename, "r")

        with contextlib.closing(fp):
            while True:
                line = fp.readline()
                if not line:
                    if not self.forever:
                        return
                    time.sleep(1)
                    continue

                ts, row = self.parser(line)
                self.analytics.record(self.event, row, ts)

def build_parser():
    parser = OptionParser(usage="Usage: %prog [options] logfile outpath")
    parser.add_option("-n", "--name", dest="name")
    parser.add_option("-p", "--parser", dest="parser", help="Parser (accesslog, ...)")
    parser.add_option("-f", "--forever", dest="forever", default=False, action="store_true")
    return parser

if __name__ == "__main__":
    parser = build_parser()
    options, args = parser.parse_args()
    if len(args) < 2:
        parser.error("must specify a log file and output path")
    if not options.parser:
        parser.error("must specify a parser")
    if not options.name:
        parser.error("must specify a name")

    parser = PARSERS[options.parser]
    parselog = ParseLog(options.name, args[0], args[1], parser, options.forever)
    parselog.run()
