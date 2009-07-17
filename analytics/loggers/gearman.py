
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        from django.utils import simplejson as json
from ..gearman import GearmanClient, GearmanWorker

class GearmanLogger(object):
    def __init__(self, servers):
        if isinstance(servers, GearmanClient):
            self.conn = servers
        else:
            self.conn = GearmanClient(servers)

    def write(self, name, timestamp, attributes):
        record = json.dumps(dict(name=name, timestamp=timestamp, attributes=attributes))
        self.conn.dispatch_background_task("analytics", record)

class WorkerHooks(object):
    def start(self, job):
        pass

    def fail(self, job, exc):
        import traceback
        traceback.print_exc(exc)

    def complete(self, job, result):
        pass

class GearmanLoggerWorker(object):
    def __init__(self, logger, servers, prefix=None):
        self.worker = GearmanWorker(job_servers=servers, prefix=prefix)
        self.worker.register_function("analytics", self.analytics)
        if hasattr(logger, '__iter__'):
            self.loggers = logger
        else:
            self.loggers = [logger]

    def analytics(self, job):
        record = json.loads(job.arg)
        record = dict((str(k), v) for k, v in record.iteritems())
        for logger in self.loggers:
            logger.write(**record)

    def work(self):
        self.worker.work(hooks=WorkerHooks())
