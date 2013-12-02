#!/bin/bash

TEST_CASE=$1

CLASSPATH=lib/\*:`hadoop classpath`
HADOOP_OPTS="-javaagent:/apps/hadoop/1.0/lib/aspectjweaver-1.7.3.jar $HADOOP_OPTS"
#export DEBUG_PORT=9000
#export DEBUG_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=$DEBUG_PORT"

if [ -n "$TEST_CASE" ]; then
    $JAVA_HOME/bin/java -cp $CLASSPATH $DEBUG_ARGS $HADOOP_OPTS com.netflix.test.TestCaseRunner com.netflix.bdp.s3mper.listing.ConsistentListingAspectTest $TEST_CASE

    RCODE=$?

    if [ $RCODE != 0 ]; then
        echo "[FAILED]: $TEST_CASE"
    else
        echo "[PASSED]: $TEST_CASE"
    fi
else
    $JAVA_HOME/bin/java -cp $CLASSPATH $DEBUG_ARGS $HADOOP_OPTS org.junit.runner.JUnitCore com.netflix.bdp.s3mper.listing.ConsistentListingAspectTest
fi
