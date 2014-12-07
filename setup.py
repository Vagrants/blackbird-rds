#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='blackbird-rds',
    version='0.1.0',
    description=(
        'Get metrics of AWS RDS by using CloudWatch API.'
    ),
    author='ARASHI, Jumpei',
    author_email='jumpei.arashi@arashike.com',
    url='https://github.com/Vagrants/blackbird-rds',
    install_requires=[
        'blackbird',
        'boto'
    ],
    classifiers=[
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ]
)
