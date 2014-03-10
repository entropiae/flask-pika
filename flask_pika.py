# -*- coding: utf-8 -*-

import pika
import logging
import contextlib

log = logging.getLogger(__name__)


class FlaskConfigMixin(object):
    """
    Wrap the configuration api used in most flask plugin; allows to write
    plugin-style class
    """

    def init_app(self, app, config_prefix):
        self.app = app

        def add_key(key):
            return '{}_{}'.format(config_prefix, key)

        def key(key):
            if key.startswith(config_prefix):
                key = key[len(config_prefix) + 1:]
            return key

        self.config = dict(
            (key(k), v)
            for k, v in app.config.iteritems()
            if k.startswith(config_prefix)
        )

        self.debug = app.debug and app.config.get(add_key('DEBUG'), True)


class Rabbit(FlaskConfigMixin):

    def __init__(self, app=None, config_prefix='RABBITMQ'):
        if app:
            self.init_app(app, config_prefix)

    def init_app(self, app, config_prefix):
        super(Rabbit, self).init_app(app, config_prefix)

        connection_params = self.config['CONNECTION_PARAMS']
        credentials = pika.PlainCredentials(
            connection_params.pop('username', 'guest'),
            connection_params.pop('password', 'guest')
        )

        self.pika_connection_params = pika.ConnectionParameters(
            credentials=credentials,
            **connection_params
        )

        self.connection = pika.BlockingConnection(self.pika_connection_params)

    @contextlib.contextmanager
    def channel(self):
        channel = self.connection.channel()
        yield channel
        channel.close()
