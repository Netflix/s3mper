S3mper
=====

S3mper is a library that provides an additional layer of consistency checking on top of Amazon's S3 index through use of a consistent, secondary index.

Overview
--------

S3mper leverages [Aspect Oriented Programming](http://en.wikipedia.org/wiki/Aspect-oriented_programming) and is implemented with [AspectJ](http://eclipse.org/aspectj/ "AspectJ") to advise implementations of the Hadoop FileSystem (primarily the NativeS3FileSystem implementation) with additional logic to crosscheck a secondary index for consistency.  

The default implementation of the secondary index uses DynamoDB because of the speed, consistency, and availability guarantees that service provides.  The table schema is designed to be light-weight and fast so as to not impair the performance of the file system.

There are two logical indexes[^1] used in the table structure to access path and file information.  The first index is path based for lookup during listing operations.  The second index is timeseries based so that entries can be expired from both indexes without having to scan the entire table.

##### Table Structure

| Hash Key: path | Range Key: file |  epoch   |   deleted  |   dir  |  linkPath   | linkFile    |
| -------------- | --------------- | -------- | ---------- | ------ | ----------- | ----------- |
| //\<bucket\>/\<path\> | \<filename\>| \<timestamp\> | \<flag\>| \<flag\> |   N/A   |     N/A     |
| epoch[^2] | \<timestamp+entropy\>|   N/A | N/A | N/A | //\<bucket\>/\<path\> | \<filename\> |

The purpose of this table scheme is to provide range query operations for directory listings and for timeseries deletes.  



[^1]: The indexes are not implemented using DynamoDB Secondary Indexes due to constrains
[^2]: This is a static value _'epoch'_ 


[Building](id:build)
--------

A gradle wrapper is used to build s3mper and can be run without additional tools.  Edit the `build.gradle` to the appropriate version of hadoop and build with the following command. 

```
$ ./gradlew release
```

This will produce the necessary jar files for use with hadoop in `build/libs` and a tar file with all dependencies for use with the admin tool.

Installing
----------

Installation requires the following steps:

 * Installing libraries on client and cluster hosts
 * Modifying the hadoop configuration to enable s3mper
 

### Library Installation

The three jar files from the `build/libs` directory need to be copied to the `$HADOOP_HOME/lib` directory on all hosts. The three files are:

```
s3mper-1.0.0.jar    
aspectjrt-1.7.3.jar           
aspectjweaver-1.7.3.jar
```

### Hadoop Configuration

Three files need to be updated to enable s3mper:

##### Changes to `$HADOOP_HOME/conf/hadoop-env.sh`

This file needs to be updated to load the aspects using a java agent.  Modify the `HADOOP_OPTS` variable like the following:
 
 ```
 export HADOOP_OPTS="-javaagent:$HADOOP_HOME/lib/aspectjweaver-1.7.3.jar $HADOOP_OPTS"
 ```
 
##### Changes to `$HADOOP_HOME/conf/core-site.xml`

S3mper is disabled by default and must be explicitly enabled with the following option:

```
<property><name>s3mper.disable</name><value>false</value></property>
```

##### Changes to `$HADOOP_HOME/conf/mapred-site.xml` [Optional]

The child processes of the task trackers need to have the java agent included in the jvm options as well.  If you updated the hadoop-env.sh on all hosts, this step should not be necessary and may cause the jvm to fail to start if there are two of the same java agent commands.  The following can be added to the `mapred-site.xml` to add the java agent if the child processes don't have the agent enabled (assumes hadoop is installed in `/opt/hadoop`):

```
<property><name>mapred.child.java.opts</name><value>--javaagent:/opt/hadoop/lib/aspectjweaver-1.7.3.jar</value></property>
```

##### Detailed Logging `$HADOOP_HOME/conf/log4j.properties` [Optional]

To turn on detailed s3mper logging to see information about what s3mper is doing, add the following line to the log4j configuration:

```
log4j.logger.com.netflix.bdp.s3mper=trace
```

 
Configuration
-------------
##### Creating the DynamoDB Table

The easiest way to create the initial metastore in DynamoDB is to set `s3mper.metastore.create=true` and execute a command against the metastore.  This will create the table in DynamoDB with the proper configuration.  The read/write unit capacity can be configured via the AWS DyanmoDB console.

##### Creating SQS Queues

The SQS Queues used for alerting need to be created by hand (no tool exists to create them automatically).  The default queue names are:

```
s3mper.alert.queue
s3mper.timeout.queue
s3mper.notification.queue
```
Messages will be delivered to these queues when a listing inconsistency is detected.


##### Configuration Options

S3mper supports a wide variety of options that can be controlled using properties from within a Pig/Hive/Hadoop job.  These options manage how s3mper will respond to a in consistent listing when it occurs.  The table below describes these options:

Property | Default | Description
-------- | ------- | -----------
s3mper.disable|FALSE|"Disables all functionality of s3mper. The aspect will still be woven, but it will not interfere with normal behavior."
s3mper.metastore.create|FALSE|Create the metastore if it doesn't exist. Note that this isn't an atomic operation so commands may fail until the metastore is fully initialized.  It's best to create the metastore prior to running any commands.
s3mper.failOnError|FALSE|"If true, any inconsistent list operation will result in a S3ConsistencyException being thrown, logging an error, and notifying CloudWatch. If false, the exception will not be thrown but the log and CloudWatch metric will still be sent."
s3mper.task.failOnError|FALSE|Controls whether a M/R Task (i.e. a Child TaskTracker process) will fail if the task fails a consistency check. Most queries only check at the start of the query.
s3mper.listing.recheck.count|15|How many times to recheck the listing. This works in combination with 's3mper.listing.recheck.period' to control how long to wait before failing/proceeding with the query.
s3mper.listing.task.recheck.count|0|How many times to recheck listing within a MapReduce task context (i.e. a Child task executing on the EMR cluster). This is handled separately from other cases because it may cause the task to timeout. In general listing is done prior to executing the task.
s3mper.listing.recheck.period|60000|How long to wait (in Milliseconds) between checks defined by 's3mper.listing.recheck.count'
s3mper.listing.task.recheck.period|0|How long to wait (in Milliseconds) between checks defined by 's3mper.listing.task.recheck.count'
s3mper.metastore.deleteMarker.enabled|FALSE|"Use a delete marker instead of removing the entry from the metastore. This will fix the second type of consistency problem where a file is deleted, but the listing still shows that it is available by removing those deleted files from the listing."
s3mper.listing.directory.tracking| FALSE | Track directory creation/deletion in the metastore.
s3mper.listing.delist.deleted|TRUE|"Removes files from the listing that have delete markers applied to them. If delete markers is enabled, this should also be enabled or the listing will expect files that are actually deleted."
s3mper.metastore.impl|\<see code\>|The fully qualified class with metastore implementation.
s3mper.dispatcher.impl|\<see code\>|The fully qualified class with alert dispatcher implementation.
fs.\<scheme\>.awsAccessKeyId||Key to use for DynamoDB access
fs.\<scheme\>.awsSecretAccessKey||Secret to use for DynamoDB access
s3mper.override.awsAccessKeyId||Key to use for DynamoDB access if different from the default key
s3mper.override.awsSecretAccessKey||Secret to use for DynamoDB access if different from the default key
s3mper.metastore.read.units|500|The number of read units to provision on create. Only used if the table does not exist.
s3mper.metastore.write.units|100|The number of write units to provision on create. Only used if the table does not exist.
s3mper.metastore.name|ConsistentListingMetastore|The name of the DynamoDB table to use. 

Verification
------------

Run `gradle test`. You will need to set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

Administration
--------------

S3mper is intended to only provide consistency guarentees for a "window" of time.  This means that entries are removed from the secondary index after a set period of time from which point the S3 index is expected to be consistent.  A commandline admin tool is provided that allows for configurable cleanup of expired entries in the secondary index.  To use the admin tool, unpack the tar file produced in the [Building](#build) section.  The command can be found at the root level and is simply `s3mper`. 

__Note:__ you many need to modify the classpath in the s3mper script to point to the s3mper lib directory from the tar file.

To run the command, use `./s3mper <meta | sqs | fs> <options>`

A cron job (or similar scheduled job) should be configured to remove expired entries in DynamoDB.  The following command can be used to delete entries older than one day:

```
./s3mper meta delete_ts -ru 100 -wu 100 -s 1 -d 10 -u days -n 1 

# delete_ts   Use the timeseries index to delete entries
# ru          Max read units to consume in DynamoDB
# wu          Max write units to consume in DynamoDB
# s           Number of scan threads to use (Note: only use 1)
# d           Number of delete threads to use
# u           Time unit
# n           Number of time units          
```

Running the cleanup on a regular basis (every 30min) will limit the cleanup work required down and keep the consistency time window well regulated.

Issues
------

Please file issues on github [here](https://github.com/Netflix/s3mper/issues "S3mper Issue Tracking")


