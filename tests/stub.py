#!/usr/bin/python

import random
import sys
import time

import subprocess32
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory

hosts = ['172.16.238.12']


keyspace = 'fedikeyspace'
table = 'test_table'
userid = 'testuser'

random.seed()
dead_node = random.randint(1, 3)
original_number = 1
new_number = 5
tries = 1000

def create_session(cl):
    sess = cl.connect()
    sess.default_consistency_level = ConsistencyLevel.ONE
    sess.row_factory = tuple_factory
    return sess

cluster = Cluster(contact_points=hosts,
                  default_retry_policy=DowngradingConsistencyRetryPolicy())

session = create_session(cluster)

kq = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }" % keyspace
session.execute(kq)
tq = "CREATE TABLE IF NOT EXISTS %s.%s (userid text PRIMARY KEY, firstname text) WITH read_repair_chance = 0 AND dclocal_read_repair_chance = 0" % (keyspace, table)
session.execute(tq)


def run_command(command):
    """Execute a shell command and return the output
    :param command: the command to be run and all of the arguments
    :returns: success_boolean, command_string, stdout, stderr
    """
    res = subprocess32.call(command)
    return res


def start_cluster():
    comm = ["docker-compose", "up", "-d"]
    res = run_command(comm)
    if res == 0:
        print "Cluster started"
        time.sleep(1)
        return True
    return False


def write_to_cluster(number):
    q = "update fedikeyspace.test_table set firstname='%d' where userid='%s'" % (number, userid)
    res = session.execute(q)
    print "writing to cluster <%d>" % number
    time.sleep(5)
    return True


def stop_node(index):
    comm = ["docker-compose", "stop", "cassandra%d" % index]
    res = run_command(comm)

    if res == 0:
        print "Node %d stopped" % index
        time.sleep(5)

        return True
    return False


def start_node(index):
    comm = ["docker-compose", "start", "cassandra%d" % index]
    res = run_command(comm)
    if res == 0:
        print "Node %d started" % index
        time.sleep(10)
        return True
    return False


def ensure_it_has(what, num_tries=100):
    print "Searching holes..."

    q = "select * from %s.%s" % (keyspace, table)
    match = 0
    for i in range(1, num_tries):
        res = session.execute(q)
        for st in res:
            if st[1] == str(what):
                print "String found %s (%d/%d)" % (st[1], match, i)
                match += 1

    return match


def make_hole():
    if start_cluster():
        if write_to_cluster(original_number):
            if stop_node(dead_node):
                if write_to_cluster(new_number):
                    if start_node(dead_node):
                        return True
    return False


if len(sys.argv) == 2 and sys.argv[1] == 'make':
    make_hole()

matches = ensure_it_has(original_number, tries)
if matches > 0:
    print "Hole found! Probability: %.2f" % (float(matches)/float(tries))
    exit(0)


print "There is no holes"
cluster.shutdown()
exit(1)
