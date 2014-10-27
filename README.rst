Incursion: InfluxDB for Developers
==================================

.. image:: https://badge.fury.io/py/incursion.png
    :target: http://badge.fury.io/py/incursion

.. image:: https://pypip.in/d/incursion/badge.png
        :target: https://crate.io/packages/incursion/

.. image:: https://travis-ci.org/voidfiles/incursion.png
        :target: https://travis-ci.org/voidfiles/incursion

Incursion is an MIT Licensed InfluxDB client, written in Python, for developers.

The existing InfluxDB python client is great. This client is built on
that but many python developers have come to expect a programmatic
method for building queries on top of a raw unstructured query interface.

Incursion was built to bring a new pattern to your InfluxDB Queries.

.. code-block:: pycon
    >>> import incursion as indb
    >>> q = indb.q('page_views')
    >>> q = q.columns(indb.count(indb.distinct('author_id')), 'author_id')
    >>> q = q.group_by(indb.time('1h'))
    >>> q = q.where(category__matches=indb.regex('/(10|11)/'))
    >>> from, to = (datetime(2014, 10, 20), datetime(2014, 10, 21))
    >>> q = q.where(time__gt=from, time__lt=to)
    >>> q = q.fill(0)
    >>> q = q.limit(None)
    >>> resp = indb.get_result(q)
    >>> assert resp['page_views']
    >>> print '%(14)s %(6)s %(2)s' % ('time', 'count', 'id')
    >>> for row in resp['page_views']:
    >>>   print '%(-14)s %(-6)s %(-2)s' % (row.time, row.count, row.author_id)
    time          count id
    1413908730239 10    1
    1413908730239 8     2
    ...


Features
--------

- Query Builder Pattern
- Lots of tests
- Safety first (all queries have a limit unless explicitly turned off)
- Ready for contributors (seriously, this should be a community project)

Installation
------------

To install Incursion, simply:

.. code-block:: bash

    $ pip install incursion

Future Features
---------------

- Continuous Query Manager (fanouts and downsampling)
- In-app joining fanout query results to source series results

Contribute
----------

#. Fork `the repository`_ on GitHub to start making your changes to the **master** branch (or branch off of it).
#. Write a test which shows that the bug was fixed or that the feature works as expected.
#. Send a pull request :) Make sure to add yourself to AUTHORS_.

.. _`the repository`: http://github.com/voidfiles/incursion
.. _AUTHORS: https://github.com/voidfiles/incursion/blob/master/AUTHORS.rst