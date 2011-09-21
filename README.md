# Cascading.Memcached

This project provides a [Cascading](http://cascading.org) Tap and Scheme for integrating with a Memcached cluster.

It provides support for writing data to a memcached protocol compatible
cluster when bound to a Cascading data processing flow, and works with both Cascading
local and Apache Hadoop planners.

See the tests for usage.

This jar relies on the [SpyMemcached ](http://code.google.com/p/spymemcached/) memcached library.
And uses the [jmemcache-daemon](http://code.google.com/p/jmemcache-daemon/) for testing.

## Building

To build a jar,

`> gradle jar`

To install the jar into the local Maven repo

`> gradle install`

To test (requires a localhost memcached cluster),

`> gradle test`

## Using with Hadoop

The cascading-memcached.jar file should be added to the "lib"
directory of your Hadoop application jar file along with all
Cascading dependencies.