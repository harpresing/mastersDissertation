#!/usr/bin/env python
'''Setuptools params'''

from setuptools import setup, find_packages

setup(
    name='proactiveHadoop',
    version='0.0.0',
    description='Implementation of proactive Scheduling System for Hadoop',
    author='Harpreet Singh',
    author_email='singhh@tcd.ie',
    packages=find_packages(exclude='test'),
    long_description="""\
MSc CS Dissertation project submitted to Trinity College Dublin, 2016
      """,
      classifiers=[
          "License :: OSI Approved :: GNU General Public License (GPL)",
          "Programming Language :: Python",
          "Development Status :: 4 - Beta",
          "Intended Audience :: Developers",
          "Topic :: Internet",
      ],
      keywords='networking protocol Internet OpenFlow data center datacenter',
      license='GPL',
      install_requires=[
        'setuptools',
        'networkx'
      ])
