
import time

class Analytics(object):
    def __init__(self, logger, global_attributes=None):
        self.global_attributes = global_attributes or {}
        self.logger = logger

    def clear_global_attributes(self):
        self.global_attributes = {}

    def set_global_attributes(self, **attributes):
        self.global_attributes.update(attributes)

    def record(self, event, timestamp=None, **attributes):
        if self.global_attributes:
            attrs = self.global_attributes.copy()
            attrs.update(attributes)
        else:
            attrs = attributes
        attrs = dict((k, v) for k, v in attrs.iteritems() if v is not None)
        self.logger.write(
            event = event,
            timestamp = int(timestamp or time.time()),
            attributes = attributes,
        )
