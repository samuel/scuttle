class TestLogger(object):
    def write(self, name, timestamp, attributes):
        print "METRICS:", timestamp, name, attributes
