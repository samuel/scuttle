class TestLogger(object):
    def write(self, event, timestamp, attributes):
        print "METRICS:", timestamp, event, attributes
