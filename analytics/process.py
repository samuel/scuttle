#!/usr/bin/env python

"""
Given a directory of parsed log files, merge them into the final logs.

<input_path>/filename.log -> <output_path>/event/year/month/day/log#####
"""

from __future__ import with_statement

import base64
import datetime
import os
import sys
import time
import uuid

def unique():
    value = uuid.uuid4().bytes
    return base64.b64encode(value, ('-', '_')).rstrip('=')

class ProcessLogs(object):
    def __init__(self, input_path):
        self.input_path = input_path
        self.input_files = os.listdir(input_path)
        self.output_files = {}
        self.max_file_size = 128*1024*1024
        self.output_path = os.path.join(self.input_path, "processed")
        self.working_path = os.path.join(self.input_path, "working")
        self.trash_path = os.path.join(self.input_path, "trash")
        for path in (self.output_path, self.working_path, self.trash_path):
            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except OSError, exc:
                    if exc.errno != 17: # If the error isn't "file exists" then reraise
                        raise

    def close_all_output_files(self):
        for base_filename, fp in self.output_files.itervalues():
            fp.close()
        self.output_files = {}

    def get_optout_file(self, event, timestamp):
        date = datetime.datetime.utcfromtimestamp(timestamp)
        base_filename = "ev=%s/dt=%s" % (event, date.strftime("%Y-%m-%d"))

        if base_filename in self.output_files:
            return self.output_files[base_filename]

        base_path = os.path.join(self.output_path, base_filename)

        if not os.path.exists(base_path):
            try:
                os.makedirs(base_path)
            except OSError, exc:
                if exc.errno != 17: # If the error isn't "file exists" then reraise
                    raise

        filename = os.path.join(base_path, "%d-%s.log" % (timestamp, unique()))
        fp = open(filename, "a")
        self.output_files[base_filename] = (base_filename, fp)

        return base_filename, fp

    def write_event(self, event, timestamp, attributes):
        base_filename, fp = self.get_optout_file(event, timestamp)
        fp.write("%s\x01%s\n" % ("%.3f" % timestamp if isinstance(timestamp, float) else timestamp, attributes))
        if fp.tell() > self.max_file_size - 1024:
            fp.close()
            del self.output_files[base_filename]

    def process_file(self, filename, path):
        working_filename = os.path.join(self.working_path, filename)
        os.rename(path, working_filename)
        time.sleep(2) # Incase someone's writing to the file give them time to notice
        with open(working_filename, "r") as fp:
            for line_num, line in enumerate(fp):
                line = line.strip()
                try:
                    timestamp, event, attributes = line.split('\x01')
                except ValueError:
                    sys.stderr.write("Failed line %d in file %s: %s\n" % (line_num, filename, repr(line)))
                    continue
                if '.' in timestamp:
                    timestamp = float(timestamp)
                else:
                    timestamp = int(timestamp)
                self.write_event(event, timestamp, attributes)
        os.rename(working_filename, os.path.join(self.trash_path, filename))
        self.close_all_output_files()

    def run(self):
        for filename in self.input_files:
            path = os.path.join(self.input_path, filename)
            if os.path.isfile(path):
                self.process_file(filename, path)

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    ProcessLogs(input_path, output_path).run()
