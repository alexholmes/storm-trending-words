# Trending Search Storm Topology

This is an example Storm topology which, given an unbounded stream of words, maintains the
top N.

# Getting started

## Prerequisites

### Download the source

Make sure you have the code available on your machine.

    $ git clone git://github.com/alexholmes/storm-trending-words.git && cd storm-strending-words


### Install Maven

Install Maven (preferably version 3.x) by following the [Maven installation instructions](http://maven.apache.org/download.cgi).

## Building

You can package a jar suitable for submitting to a Storm cluster with the command:

    $ mvn package

This will package your code and all the non-Storm dependencies into a single "uberjar" at the path
`target/storm-starter-{version}-jar-with-dependencies.jar`.


## Running unit tests

Use the following Maven command to run the unit tests that ship with storm-starter.  Unfortunately `lein test` does not
yet run the included unit tests.

    $ mvn -f m2-pom.xml test


# Running

To run in "local" mode (i.e. within the client JVM):

    $ storm jar target/storm-trending-words-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.trending.TrendingWordsTopology

To run on a remote Storm cluster, simply add an argument which is the name of the topology:

    $ storm jar target/storm-trending-words-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.trending.TrendingWordsTopology trending-search
