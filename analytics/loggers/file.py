
import os
import time
from urllib import quote_plus
from hashlib import md5

from analytics.utils import encode_value

def encode_value(value):
    if isinstance(value, basestring):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
    elif isinstance(value, bool):
        value = str(int(value))
    elif isinstance(value, (int, long)):
        value = str(value)
    elif isinstance(value, float):
        value = "%.5f" % value
    elif isinstance(value, datetime.timedelta):
        value = str(int(value.days*24*60*60 + value.seconds))
    elif isinstance(value, datetime.datetime):
        value = str(int(time.mktime(value.timetuple())))
    elif isinstance(value, (list, tuple)):
        value = ",".join(value)
    else:
        raise TypeError("Unsupported attribute value of type %s" % type(value))

    return value

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

    def write(self, event, timestamp, attributes):
        self.open() # no-op if already opened

        attrs = "\x02".join(
            "%s\x03%s" % (n, encode_value(v).replace('\n', '\\n').replace('\x03', '?').replace('\x02', '?').replace('\x01', '?'))
            for n, v in attributes.iteritems())

        self.fp.write("\x01".join((
                str(timestamp),
                quote_plus(event),
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
        start_time = int(fp.read(15).split('\x01', 1)[0])
        fp.close()

        if size > 1024*1024 and size > self.rotate_size or time.time() - start_time >= self.rotate_period:
            if not os.path.exists(self.transfer_path):
                os.mkdir(self.transfer_path)

            unique = md5("%f%d%d" % (time.time(), start_time, size)).hexdigest()
            name = "%d_%d_%s.log" % (start_time, time.time(), unique)

            os.rename(self.current_log_path, os.path.join(self.transfer_path, name))
