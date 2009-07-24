
import time

class Analytics(object):
    def __init__(self, logger, global_attributes=None):
        self.global_attributes = global_attributes or {}
        self.logger = logger

    def record(self, event, timestamp=None, **attributes):
        if self.global_attributes:
            attrs = self.global_attributes.copy()
            attrs.update(attributes)
        else:
            attrs = attributes
        self.logger.write(
            event = event,
            timestamp = int(timestamp or time.time()),
            attributes = attributes,
        )
