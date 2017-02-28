#!/usr/bin/env python
# coding: utf-8
# Copyright 2009 Alexandre Fiori
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import setuptools

py_version = sys.version_info[:2]
twisted_version = py_version == (2, 6) and 'twisted<15.5' or 'twisted'

setuptools.setup(
    name="txredisapi",
    version="1.4.4",
    py_modules=["txredisapi"],
    install_requires=[twisted_version, "six"],
    author="Alexandre Fiori",
    author_email="fiorix@gmail.com",
    url="http://github.com/fiorix/txredisapi",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="non-blocking redis client for python",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
