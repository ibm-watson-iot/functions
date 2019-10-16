import logging
import os
import errno
import sys

logger = logging.getLogger(__name__)

try:
    from confluent_kafka import Producer
except ImportError:
    logger.warning('Warning: confluent_kafka is not installed. Publish to MessageHub not supported.')
    KAFKA_INSTALLED = False
else:
    KAFKA_INSTALLED = True

from .util import randomword

FLUSH_PRODUCER_EVERY = 100

MH_USER = os.environ.get('MH_USER')
MH_PASSWORD = os.environ.get('MH_PASSWORD')
MH_BROKERS_SASL = os.environ.get('MH_BROKERS_SASL')
MH_DEFAULT_ALERT_TOPIC = os.environ.get('MH_DEFAULT_ALERT_TOPIC')
MH_CLIENT_ID = 'as-pypeline-alerts-producer'


class MessageHub:
    MH_CA_CERT_PATH = '/etc/ssl/certs/eventstreams.pem'
    MH_CA_CERT = os.environ.get('MH_CA_CERT')

    def _delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.warning('Message delivery failed: {}'.format(err))

    # else:
    #     logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce_batch(self, msg_and_keys):
        self.produce_batch(MH_DEFAULT_ALERT_TOPIC, msg_and_keys)

    def produce_batch(self, topic, msg_and_keys):
        if topic is None or len(topic) == 0 or msg_and_keys is None:
            return

        cnt = 0
        producer = None
        for msg, key in msg_and_keys:
            producer = self.produce(topic, msg=msg, key=key, producer=producer)
            cnt += 1
            if cnt % FLUSH_PRODUCER_EVERY == 0:
                # Wait for any outstanding messages to be delivered and delivery report
                # callbacks to be triggered.
                producer.flush()
        if producer is not None:
            producer.flush()

    def produce(self, topic, msg, key=None, producer=None, callback=_delivery_report):
        if topic is None or len(topic) == 0 or msg is None:
            return

        options = {'sasl.username': MH_USER, 'sasl.password': MH_PASSWORD, 'bootstrap.servers': MH_BROKERS_SASL,
                   'security.protocol': 'SASL_SSL',  # 'ssl.ca.location': '/etc/ssl/certs/', # ubuntu
                   'ssl.ca.location': self.MH_CA_CERT_PATH, 'sasl.mechanisms': 'PLAIN', 'api.version.request': True,
                   'broker.version.fallback': '0.10.2.1', 'log.connection.close': False,
                   'client.id': MH_CLIENT_ID + '-' + randomword(4)}

        if KAFKA_INSTALLED:
            if producer is None:
                producer = Producer(options)

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            producer.produce(topic, value=msg, key=key, callback=callback)

            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        # producer.flush()

        else:

            logger.info('Topic %s : %s' % (topic, msg))

        return producer

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def safe_open_w(self):
        '''
        Open "MH_CA_CERT_PATH" for writing, creating the parent directories as needed.
        '''
        self.mkdir_p(os.path.dirname(self.MH_CA_CERT_PATH))
        return open(self.MH_CA_CERT_PATH, 'w')

    def __init__(self):
        try:
            exists = os.path.isfile(self.MH_CA_CERT_PATH)
            if exists:
                logger.info('EventStreams certificate exists')
            else:
                if self.MH_CA_CERT is not None and len(self.MH_CA_CERT) > 0 and self.MH_CA_CERT.lower() != 'null':
                    logger.info('EventStreams create ca certificate file in pem format.')
                    with self.safe_open_w() as f:
                        f.write(self.MH_CA_CERT)
                else:
                    logger.info('EventStreams ca certificate is empty.')
                    if sys.platform == "darwin":  # MAC OS
                        self.MH_CA_CERT_PATH = '/etc/ssl/cert.pem'
                    else:
                        self.MH_CA_CERT_PATH = '/etc/ssl/certs'
        except Exception as ex:
            logger.error('Initialization of EventStreams failed.')
            raise
