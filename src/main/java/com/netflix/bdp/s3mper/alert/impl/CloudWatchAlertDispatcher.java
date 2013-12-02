/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */


package com.netflix.bdp.s3mper.alert.impl;

import com.netflix.bdp.s3mper.alert.AlertDispatcher;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.netflix.bdp.s3mper.alert.impl.AbstractMessage.QueryType;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Dispatches CloudWatch Metrics and SQS Messages on the event of consistency
 * failure or timeout.
 * 
 * @author dweeks
 */
public class CloudWatchAlertDispatcher implements AlertDispatcher {
    private static final Logger log = Logger.getLogger(CloudWatchAlertDispatcher.class.getName());
    
    private AmazonCloudWatchAsyncClient cloudWatch;
    private AmazonSQSClient sqs;
    
    private String namespace = "netflix";
    private String cloudWatchConsistencyMetric = "com.netflix.bdp.s3mper.consistency.failures";
    private String cloudWatchTimeoutMetric = "com.netflix.bdp.s3mper.consistency.timeout";
    
    private String consistencyQueue = "s3mper-alert-queue";
    private String timeoutQueue = "s3mper-timeout-queue";
    private String notificationQueue = "s3mper-notification-queue";
    
    private String consistencyQueueUrl = "";
    private String timeoutQueueUrl = "";
    private String notificationQueueUrl = "";
    
    private boolean reportingDisabled = false;
    
    private URI uri;
    private Configuration conf;
    
    private int pathReportLimit = 10;
    private int traceDepth = 15;
    
    @Override
    public void init(URI uri, Configuration conf) {
        this.uri = uri;
        this.conf = conf;
    }
    
    /**
     * Don't initialize the SQS queues unless we actually need to send a message.
     */
    private void lazyInit() {
        String keyId = conf.get("fs."+uri.getScheme()+".awsAccessKeyId");
        String keySecret = conf.get("fs."+uri.getScheme()+".awsSecretAccessKey");
        
        //An override option for accessing across accounts
        keyId = conf.get("fs."+uri.getScheme()+".override.awsAccessKeyId", keyId);
        keySecret = conf.get("fs."+uri.getScheme()+".override.awsSecretAccessKey", keySecret);
        
        synchronized(this) {
            if(cloudWatch == null) {
                initCloudWatch(keyId, keySecret);
            }
            
            if(sqs == null) {
                initSqs(keyId, keySecret);
            }
        }
        
    }
    
    private void initCloudWatch(String keyId, String keySecret) {
        log.debug("Initializing CloudWatch Client");
        cloudWatch = new AmazonCloudWatchAsyncClient(new BasicAWSCredentials(keyId, keySecret));
    }
    
    private void initSqs(String keyId, String keySecret) {
        log.debug("Initializing SQS Client");
        sqs = new AmazonSQSClient(new BasicAWSCredentials(keyId, keySecret));
        
        //SQS Consistency Queue
        consistencyQueue = conf.get("s3mper.alert.sqs.queue", consistencyQueue);
        consistencyQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest(consistencyQueue)).getQueueUrl();
        
        //SQS Timeout Queue
        timeoutQueue = conf.get("s3mper.timeout.sqs.queue", timeoutQueue);
        timeoutQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest(timeoutQueue)).getQueueUrl();
        
        //SQS Notification Queue
        notificationQueue = conf.get("s3mper.notification.sqs.queue", notificationQueue);
        notificationQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest(notificationQueue)).getQueueUrl();
        
        //Disable reporting  (Testing purposes mostly)
        reportingDisabled = conf.getBoolean("s3mper.reporting.disabled", reportingDisabled);
    }
    
    /**
     * Sends an alert detailing that the given paths were missing from a list
     * operation.
     * 
     * @param missingPaths 
     */
    @Override
    public void alert(List<Path> missingPaths) {
        lazyInit();
        
        if(reportingDisabled) {
            return;
        }
        
        sendCloudWatchConsistencyAlert();
        sendSQSConsistencyMessage(missingPaths, false);
    }

    private void sendCloudWatchConsistencyAlert() {
        MetricDatum datum = new MetricDatum();
        datum.setMetricName(cloudWatchConsistencyMetric);
        datum.setUnit(StandardUnit.Count);
        datum.setValue(1.0);
        
        PutMetricDataRequest request = new PutMetricDataRequest();
        request.setNamespace(namespace);
        request.setMetricData(Collections.singleton(datum));
        
        cloudWatch.putMetricData(request);
    }
     
    /**
     * Sends a message that a listing was initially inconsistent but was
     * recovered by delaying/retrying.
     * 
     * @param paths 
     */
    @Override
    public void recovered(List<Path> paths) {
        lazyInit();
        
        if(reportingDisabled) {
            return;
        }
        
        sendSQSConsistencyMessage(paths, true);
    }   
    
    private void sendSQSConsistencyMessage(List<Path> paths, boolean recovered) {
        S3ConsistencyMessage message = new S3ConsistencyMessage();
        
        buildMessage(message);
        
        List<String> pathStrings = new ArrayList<String>();
        boolean truncated = false;
        
        for(Path p : paths) {
            pathStrings.add(p.toUri().toString());
            
            //Truncate if the message payload gets to be too large (i.e. to many missing files)
            if(pathStrings.size() >= pathReportLimit) {
                truncated = true;
                break;
            }
        }
        
        message.setPaths(pathStrings);
        message.setTruncated(truncated);
        
        int missingFiles = paths.size();
        
        if(recovered) {
            missingFiles = 0;
        }
        
        message.setMissingFiles(missingFiles);
        message.setRecovered(recovered);
        
        sendMessage(consistencyQueueUrl, message);
        
        if(!recovered) {
            sendMessage(notificationQueueUrl, message);
        }
    }

    /**
     * Sends a message to the timeout queue indicating that a dynamodb operation
     * timedout.
     * 
     * @param operation
     * @param paths 
     */
    @Override
    public void timeout(String operation, List<Path> paths) {
        lazyInit();
        
        if(reportingDisabled) {
            return;
        }
        
        //TODO: Being over-protective about these timeout messages.
        try {
            sendCloudWatchTimeoutAlert();
        } catch (Exception e) {
            log.error("Failed to send cloud watch timeout alert.", e);
        }
        
        try {
            sendSQSTimeoutMessage(operation);
        } catch (Exception e) {
            log.error("Filed to send SQS timeout message.", e);
        }
    }
   
    private void sendCloudWatchTimeoutAlert() {
        MetricDatum datum = new MetricDatum();
        datum.setMetricName(cloudWatchTimeoutMetric);
        datum.setUnit(StandardUnit.Count);
        datum.setValue(1.0);
        
        PutMetricDataRequest request = new PutMetricDataRequest();
        request.setNamespace(namespace);
        request.setMetricData(Collections.singleton(datum));
        
        cloudWatch.putMetricData(request);
    }
    
    private void sendSQSTimeoutMessage(String operation) {
        S3mperTimeoutMessage message = new S3mperTimeoutMessage();
        buildMessage(message);
        
        message.setOperation(operation);
        
        sendMessage(timeoutQueueUrl, message);
    }
    
    private void buildMessage(AbstractMessage message) {
        String hostname = "unknown";
        
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Failed to identify hostname",e);
        }
    
        message.setEpoch(System.currentTimeMillis());
        message.setTimestamp(new Date(message.getEpoch()).toString());
        message.setHostname(hostname);
        
        String username = conf.get("user.name", System.getProperty("user.name"));
        
        try {
            username = UserGroupInformation.getCurrentUser().getUserName();    
        } catch (IOException e) {
            log.warn("Failed to identify user using hadoop library.", e);
        }
        
        message.setUsername(username);
        
        message.setGenieId(conf.get("genie.job.id"));
        message.setDataovenId(conf.get("dataoven.job.id"));
        String queryId = conf.get("hive.query.id");
        
        QueryType queryType = QueryType.Unknown;
        
        if(queryId != null) {
            queryType = QueryType.Hive; 
            message.setLogFile(conf.get("hive.log.file"));
        } else {
            queryId = conf.get("pig.script.id");
                
            if(queryId != null) {
                queryType = QueryType.Pig;
                message.setLogFile(conf.get("pig.logfile"));
            }
        }
        
        message.setQueryId(queryId);
        message.setQueryType(queryType);
        
        message.setJobId(conf.get("mapred.job.id"));
        message.setTaskId(conf.get("mapred.tip.id"));
        message.setAttemptId(conf.get("mapred.task.id"));
        message.setInputFile(conf.get("mapred.input.file"));
        message.setEmail(conf.get("s3mper.email"));
        
        try {
            //We have to guess at this since it may not be explicitly in the config
            if(message.getJobId() == null) {
                String[] split = conf.get("mapreduce.job.dir").split("/");
                String jobId = split[split.length - 1];

                message.setJobId(jobId);
            }
        } catch (RuntimeException e) {
            log.debug("Failed to determine job id");
        }
        
        try {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            
            List<String> stackTrace = new ArrayList<String>(traceDepth);
            
            for (int i = 0; i < traceDepth && i < stack.length; i++) {
                stackTrace.add(stack[i].toString());
            }
            
            message.setStackTrace(stackTrace);
        } catch (Exception e) {
            log.debug("Stacktrace generation failed", e);
        }
        
    }

    private void sendMessage(String url, AbstractMessage message) {
        SendMessageRequest sqsRequest = null;
        
        try {
            String payload = new ObjectMapper().writeValueAsString(message);
        
            if(log.isDebugEnabled()) {
                log.debug("Sending SQS: " + payload);
            }
            
            sqsRequest = new SendMessageRequest(url, payload);
        } catch (IOException e) {
            log.error("Failed to map json object.", e);
        }
        
        sqs.sendMessage(sqsRequest);
    }
    
    @Override
    public void setConfig(Configuration conf) {
        this.conf = conf;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }
    
}
