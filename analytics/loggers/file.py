
import os
import time
from urllib import quote_plus
from hashlib import md5

from analytics.utils import encode_value

TYPE_MAP = dict(
    float = 'f',
    integer = 'i',
    string = 's',
    bool = 'b',
    duration = 'd',
    timestamp = 't',
    url = 'u',
    tags = 'k',
    ip_address = 'a',
)

class FileLogger(object):
    def __init__(self, path, rotate_period=30*60, rotate_size=50*1024*1024):
        self.fp = None
        self.path = path
        self.current_log_path = os.path.join(path, "analytics.log")
        self.transfer_path = os.path.join(path, "transfer")
        self.rotate_period = rotate_period
        self.rotate_size = rotate_size
        self.last_rotate_check = 0

    def open(self):
        if not self.fp:
            self.fp = open(self.current_log_path, "a")

    def close(self):
        if self.fp:
            self.fp.close()
            self.fp = None

    def write(self, name, timestamp, attributes):
        self.open() # no-op if already opened

        attrs = "&".join(
            self.encode_attribute(n, v)
            for n, v in attributes.iteritems())
        
        self.fp.write("\t".join((
                str(timestamp),
                quote_plus(name),
                attrs,
            )) + "\n")

        if time.time() - self.last_rotate_check > 60*5:
            self.rotate()

    def flush(self):
        if self.fp:
            self.fp.flush()

    def rotate(self):
        if not os.path.exists(self.current_log_path):
            return False

        self.flush()

        size = os.path.getsize(self.current_log_path)
        if size < 12:
            return False

        self.close()

        fp = open(self.current_log_path, "r")
        start_time = int(fp.read(15).split('\t', 1)[0])
        fp.close()

        if size > 1024*1024 and size > self.rotate_size or time.time() - start_time >= self.rotate_period:
            if not os.path.exists(self.transfer_path):
                os.mkdir(self.transfer_path)

            unique = md5("%f%d%d" % (time.time(), start_time, size)).hexdigest()
            name = "%s_%d_%d.log" % (unique, start_time, time.time())

            os.rename(self.current_log_path, os.path.join(self.transfer_path, name))

    def encode_attribute(self, name, value):
        value, typ = encode_value(value)
        return "%s.%s=%s" % (quote_plus(name), TYPE_MAP[typ], quote_plus(value))
