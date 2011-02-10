
from kombu import BrokerConnection, Exchange, Queue, Consumer, Producer

class KombuLogger(object):
    def __init__(self, host="localhost", user="guest", password="guest", vhost="/", exchange="analytics"):
        self.connection = BrokerConnection(host, user, password, vhost)
        self.channel = self.connection.channel()
        self.exchange = Exchange(exchange, "topic", durable=True, auto_delete=False)
        self.producer = Producer(self.channel, exchange=self.exchange, serializer="json")
    
    def write(self, event, timestamp, attributes):
        self.producer.publish({"event": event, "ts": timestamp, "attr": attributes}, routing_key=event)

class KombuWorker(object):
    def __init__(self, logger, host="localhost", user="guest", password="guest", vhost="/", exchange="analytics", queue="analytics.logger"):
        self.logger = logger
        self.connection = BrokerConnection(host, user, password, vhost)
        self.channel = self.connection.channel()
        self.exchange = Exchange(exchange, "topic", durable=True, auto_delete=False)
        self.queue = Queue(queue, exchange=self.exchange, routing_key="#", auto_delete=False, durable=True, exclusive=False)
    
    def work(self):
        consumer = Consumer(self.channel, self.queue, callbacks=[self._msg_callback])
        consumer.consume()
        while True:
            try:
                self.connection.drain_events()
            except KeyboardInterrupt:
                return
    
    def _msg_callback(self, body, message):
        self.logger.write(body["event"], body["ts"], body["attr"])
        message.ack()
