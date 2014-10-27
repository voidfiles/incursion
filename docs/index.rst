.. Incursion documentation master file, created by
   sphinx-quickstart on Mon Oct 27 14:56:41 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Incursion: InfluxDB for Developers
==================================

Release v\ |version|.

Incursion is an :ref:`MIT Licensed <mit>` InfluxDB client, written in Python, for developers.

The existing InfluxDB python client is great. This client is built on
that but many python developers have come to expect a programmatic
method for building queries on top of a raw unstructured query interface.

Incursion was built to bring a new pattern to your InfluxDB Queries.


Features
--------

- Query Builder Pattern
- Continuous Query Planner
- Lots of tests
- Safety first (all queries have a limit unless explicitly turned off)
- Ready for contributors (seriously, this should be a community project)


Installation
--------------
::

    pip install incursion

You may also use Git to clone the repository from
Github and install it manually::

    git clone https://github.com/voidfiles/incursion.git
    python setup.py install

Quick Start
-----------

Incursion aims to be an easy-to-use Python client for InfluxDB.

.. code-block:: python

    import incursion as indb

    q = indb.q('page_views')
    q = q.columns(indb.count(indb.distinct('author_id')), 'author_id')
    q = q.group_by(indb.time('1h'))
    q = q.where(category__matches=indb.regex('/(10|11)/'))
    from, to = (datetime(2014, 10, 20), datetime(2014, 10, 21))
    q = q.where(time__gt=from, time__lt=to)
    q = q.fill(0)
    q = q.limit(None)

    resp = indb.get_result(q)

    assert resp['page_views'] # The response is a dict

    print '%(14)s %(6)s %(2)s' % ('time', 'count', 'id')
    for row in resp['page_views']:
      print '%(-14)s %(-6)s %(-2)s' % (row.time, row.count, row.author_id)

    # time          count id
    # 1413908730239 10    1
    # 1413908730239 8     2
    # ...


Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

