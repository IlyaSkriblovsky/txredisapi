=======
TxRedis
=======
:Info: See `the redis site <http://code.google.com/p/redis/>`_ for more information. See `github <http://github.com/fiorix/txredis/tree>`_ for the latest source.
:Author: Alexandre Fiori <fiorix@gmail.com>

About
=====
An asynchronous Python client for the Redis database, based on Twisted.

The ``txredis`` package is an improvement of the original `redis protocol
for twisted <http://code.google.com/p/redis/>`_, written by Ludovico Magnocavallo.

The `Redis Command Reference <http://code.google.com/p/redis/wiki/CommandReference>`_ is
the same in both packages.

Installation
============
You need `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_
in order to get ``txredis`` installed.
Just run **python setup.py install**

Unit tests are also included, based on `TwistedTrial <http://twistedmatrix.com/trac/wiki/TwistedTrial>`_.
Just run **trial tests** (make sure redis is running!)

Examples
========
There are some examples of using ``txredis`` in the *examples/* directory.
You need `TwistedWeb <http://twistedmatrix.com/trac/wiki/TwistedWeb>`_ or `Cyclone <http://github.com/fiorix/tornado>`_ to see it in action!

Credits
=======
Thanks to (in no particular order):

- Gleicon Moraes

  - Testing and using it in the ``RestMQ`` package

- Ludovico Magnocavallo

  - The author of the original ``redis protocol for twisted``.
