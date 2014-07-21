#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch AWS RDS metrics.
"""

import datetime

from boto.ec2 import cloudwatch

from blackbird.plugins import base


class ConcreteJob(base.JobBase):

    def __init__(self, options, queue=None, logger=None):
        super(ConcreteJob, self).__init__(options, queue, logger)
        self.metrics_config = [
            {'BinLogDiskUsage': 'Average'},
            {'CPUUtilization': 'Average'},
            {'DatabaseConnections': 'Average'},
            {'DiskQueueDepth': 'Average'},
            {'FreeStorageSpace': 'Average'},
            {'FreeableMemory': 'Average'},
            {'NetworkReceiveThroughput': 'Average'},
            {'NetworkTransmitThroughput': 'Average'},
            {'ReplicaLag': 'Average'},
            {'SwapUsage': 'Average'},
            {'ReadIOPS': 'Average'},
            {'WriteIOPS': 'Average'},
            {'ReadLatency': 'Average'},
            {'WriteLatency': 'Average'},
            {'ReadThroughput': 'Average'},
            {'WriteThroughput': 'Average'},
        ]

    def _create_connection(self):
        conn = cloudwatch.connect_to_region(
            self.options.get('region_name'),
            aws_access_key_id=self.options.get(
                'aws_access_key_id'
            ),
            aws_secret_access_key=self.options.get(
                'aws_secret_access_key'
            )
        )
        return conn

    def _fetch_metrics(self):
        conn = self._create_connection()
        result = list()

        ignore_metrics = self.options.get('ignore_metrics', list())
        period = int(self.options.get('interval', 60))
        if period <= 60:
            period = 60
            delta_seconds = 120
        else:
            delta_seconds = period
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(
            seconds=delta_seconds
        )
        dimensions = {
            'DBInstanceIdentifier': self.options.get(
                'db_instance_identifier'
            )
        }
        hostname = self.options.get('hostname')

        for entry in self.metrics_config:
            for metric_name, statistics in entry.iteritems():
                if not metric_name in ignore_metrics:
                    metric = conn.get_metric_statistics(
                        period=period,
                        start_time=start_time,
                        end_time=end_time,
                        metric_name=metric_name,
                        namespace='AWS/RDS',
                        statistics=statistics,
                        dimensions=dimensions
                    )
                    if len(metric) > 0:
                        result.append(RDSItem(
                            key=metric_name,
                            value=str(metric[0][statistics]),
                            host=hostname
                        ))

        conn.close()
        return result

    def _build_ping_item(self):
        return BlackbirdItem(
            key='rds.ping',
            value=1,
            host=self.options.get(
                'hostname'
            )
        )

    def build_items(self):
        """
        Main loop
        """
        items = list()
        items.extend(self._fetch_metrics())
        items.append(self._build_ping_item())

        for entry in items:
            if self.enqueue(entry):
                self.logger.debug(
                    'Enqueued item. {0}'
                    ''.format(entry.data)
                )


class Validator(base.ValidatorBase):
    """
    Validate configuration object.
    """
    def __init__(self):
        self.__spec = None

    @property
    def spec(self):
        self.__spec = (
            "[{0}]".format(__name__),
            "region_name = string(default='us-east-1')",
            "aws_access_key_id = string()",
            "aws_secret_access_key = string()",
            "db_instance_identifier = string()",
            "hostname = string()",
            "ignore_metrics = list(default=list())"
        )
        return self.__spec


class RDSItem(base.ItemBase):
    """
    Enqueued item.
    """

    def __init__(self, key, value, host):
        super(RDSItem, self).__init__(key, value, host)

        self.__data = dict()
        self._generate()

    @property
    def data(self):
        """
        Dequeued data.
        """
        return self.__data

    def _generate(self):
        self.__data['key'] = 'cloudwatch.rds.{0}'.format(self.key)
        self.__data['value'] = self.value
        self.__data['host'] = self.host
        self.__data['clock'] = self.clock


class BlackbirdItem(base.ItemBase):
    """
    Enqueued item.
    """

    def __init__(self, key, value, host):
        super(BlackbirdItem, self).__init__(key, value, host)

        self.__data = dict()
        self._generate()

    @property
    def data(self):
        """
        Dequeued data.
        """
        return self.__data

    def _generate(self):
        self.__data['key'] = 'blackbird.{0}'.format(self.key)
        self.__data['value'] = self.value
        self.__data['host'] = self.host
        self.__data['clock'] = self.clock


if __name__ == '__main__':
    import json
    OPTIONS = {
        'region_name': 'ap-northeast-1',
        'aws_access_key_id': 'YOUR_AWS_ACCESS_KEY_ID',
        'aws_secret_access_key': 'AWS_SECRET_ACCESS_KEY',
        'db_instance_identifier': 'YOUR_DB_INSTANCE_IDENTIFIER',
        'interval': 60,
        'ignore_metrics': list()
    }
    RESULTS = list()
    JOB = ConcreteJob(options=OPTIONS)
    METRICS = JOB._fetch_metrics()
    for ENTRY in METRICS:
        RESULTS.append(
            {
                ENTRY.key: ENTRY.value
            }
        )

    print(json.dumps(RESULTS))
