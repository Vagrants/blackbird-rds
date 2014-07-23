#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch AWS RDS metrics.
"""

import datetime

from boto.ec2 import cloudwatch
from boto import rds

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
        self.instance_classes = {
            'db.t1.micro': {
                'vCPU': 1,
                'ECU': 1,
                'Memory': 0.615
            },
            'db.m1.small': {
                'vCPU': 1,
                'ECU': 1,
                'Memory': 1.7
            },
            'db.m3.medium': {
                'vCPU': 1,
                'ECU': 3,
                'Memory': 3.75
            },
            'db.m3.large': {
                'vCPU': 2,
                'ECU': 6.5,
                'Memory': 7.5
            },
            'db.m3.xlarge': {
                'vCPU': 4,
                'ECU': 13,
                'Memory': 15
            },
            'db.m3.2xlarge': {
                'vCPU': 8,
                'ECU': 26,
                'Memory': 30
            },
            'db.r3.large': {
                'vCPU': 2,
                'ECU': 6.5,
                'Memory': 15
            },
            'db.r3.xlarge': {
                'vCPU': 4,
                'ECU': 13,
                'Memory': 30.5
            },
            'db.r3.2xlarge': {
                'vCPU': 8,
                'ECU': 26,
                'Memory': 61
            },
            'db.r3.4xlarge': {
                'vCPU': 16,
                'ECU': 52,
                'Memory': 122
            },
            'db.r3.8xlarge': {
                'vCPU': 32,
                'ECU': 104,
                'Memory': 244
            },
            'db.m2.xlarge': {
                'vCPU': 2,
                'ECU': 6.5,
                'Memory': 17.1
            },
            'db.m2.2xlarge': {
                'vCPU': 4,
                'ECU': 13,
                'Memory': 34.2
            },
            'db.m2.4xlarge': {
                'vCPU': 8,
                'ECU': 26,
                'Memory': 68.4
            },
            'db.cr1.8xlarge': {
                'vCPU': 32,
                'ECU': 88,
                'Memory': 244
            },
            'db.m1.medium': {
                'vCPU': 1,
                'ECU': 2,
                'Memory': 3.75
            },
            'db.m1.large': {
                'vCPU': 2,
                'ECU': 4,
                'Memory': 7.5
            },
            'db.m1.xlarge': {
                'vCPU': 4,
                'ECU': 8,
                'Memory': 15
            },
        }

    def _create_rds_connection(self):
        conn = rds.connect_to_region(
            self.options.get('region_name'),
            aws_access_key_id=self.options.get(
                'aws_access_key_id'
            ),
            aws_secret_access_key=self.options.get(
                'aws_secret_access_key'
            )
        )
        return conn

    def _create_cloudwatch_connection(self):
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

    def _fetch_instance(self):
        """
        Fetch RDS instance information by using boto.rds.
        This method fetch following information.
        * Allocate Disk Size
        * Allocate Memory Size(by RDS instance class)
        """
        result = list()
        conn = self._create_rds_connection()
        hostname = self.options.get('hostname')

        rds_instance = conn.get_all_dbinstances(
            instance_id=self.options.get('db_instance_identifier')
        )[0]
        allocated_storage = getattr(rds_instance, 'allocated_storage', None)
        instance_class = getattr(rds_instance, 'instance_class', None)
        if instance_class in self.instance_classes:
            allocated_memory = self.instance_classes[instance_class]['Memory']
        else:
            allocated_memory = None

        if allocated_storage is not None:
            allocated_storage = allocated_storage * 1024 * 1024 * 1024
            result.append(
                RDSItem(
                    key='storage.size[total]',
                    value=str(allocated_storage),
                    host=hostname
                )
            )
        if allocated_memory is not None:
            allocated_memory = allocated_memory * 1024 * 1024 * 1024
            result.append(
                RDSItem(
                    key='memory.size[total]',
                    value=str(allocated_memory),
                    host=hostname
                )
            )
        return result

    def _fetch_cloudwatch_metrics(self):
        conn = self._create_cloudwatch_connection()
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
                        result.append(CloudWatchRDSItem(
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
        items.extend(self._fetch_cloudwatch_metrics())
        items.extend(self._fetch_instance())
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
        self.__data['key'] = 'rds.{0}'.format(self.key)
        self.__data['value'] = self.value
        self.__data['host'] = self.host
        self.__data['clock'] = self.clock


class CloudWatchRDSItem(base.ItemBase):
    """
    Enqueued item.
    """

    def __init__(self, key, value, host):
        super(CloudWatchRDSItem, self).__init__(key, value, host)

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
    JOB = ConcreteJob(options=OPTIONS)
    METRICS = JOB._fetch_cloudwatch_metrics()
    METRICS.extend(JOB._fetch_instance())
    RESULTS = [
        ENTRY.data for ENTRY in METRICS
    ]

    print(json.dumps(RESULTS))
