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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static java.lang.String.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Simple utility for writing out logs from SQS queue and deleting messages.
 * 
 * @author dweeks
 */
public class AlertJanitor {
    private static final Logger log = Logger.getLogger(AlertJanitor.class.getName());
    
    private String consistencyQueue = "s3mper-alert-queue";
    private String timeoutQueue = "s3mper-timeout-queue";
    
    private Configuration conf;
    private AmazonSQSClient sqs;
    
    private int batchCount = Integer.getInteger("s3mper.alert.janitor.batch.count", 10);
    
    public void initalize(URI uri, Configuration conf) {
        this.conf = conf;
        
        String keyId = conf.get("fs."+uri.getScheme()+".awsAccessKeyId");
        String keySecret = conf.get("fs."+uri.getScheme()+".awsSecretAccessKey");
        
        //An override option for accessing across accounts
        keyId = conf.get("fs."+uri.getScheme()+".override.awsAccessKeyId", keyId);
        keySecret = conf.get("fs."+uri.getScheme()+".override.awsSecretAccessKey", keySecret);
        
        sqs = new AmazonSQSClient(new BasicAWSCredentials(keyId, keySecret));
        
        //SQS Consistency Queue
        consistencyQueue = conf.get("fs"+uri.getScheme()+".alert.sqs.queue", consistencyQueue);
        consistencyQueue = sqs.getQueueUrl(new GetQueueUrlRequest(consistencyQueue)).getQueueUrl();
        
        //SQS Timeout Queue
        timeoutQueue = conf.get("fs"+uri.getScheme()+".timeout.sqs.queue", timeoutQueue);
        timeoutQueue = sqs.getQueueUrl(new GetQueueUrlRequest(timeoutQueue)).getQueueUrl();
    }
    
    /**
     * Deletes all messages in the given queue.
     * 
     * @param queue 
     */
    public void clearAll(String queue) {
        do {
            List<Message> messages = pull(queue, batchCount);
 
            if(messages.isEmpty()) {
                break;
            }
            
            delete(queue, messages);
        } while(true);
    }
    
    /**
     * Writes out logs to the given path as a separate JSON message per line.
     * 
     * @param queue
     * @param path
     * @throws IOException 
     */
    public void writeLogs(String queue, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        DataOutputStream fout = fs.create(path);
        
        do {
            List<Message> messages = pull(queue, batchCount);

            if(messages.isEmpty()) {
                break;
            }
            
            for(Message m : messages) {
                fout.write((m.getBody().replaceAll("[\n|\r]", " ")+"\n").getBytes("UTF8"));
            }

            delete(queue, messages);
        } while(true);
        
        fout.close();
        fs.close();
    }
    
    private void delete(String queue, List<Message> messages) {
        List<DeleteMessageBatchRequestEntry> deleteRequests = new ArrayList<DeleteMessageBatchRequestEntry>(); 

        for(Message m : messages) {
            deleteRequests.add(new DeleteMessageBatchRequestEntry().withId(m.getMessageId()).withReceiptHandle(m.getReceiptHandle()));
        }

        log.info(format("Deleting %s messages", deleteRequests.size()));

        DeleteMessageBatchRequest batchDelete = new DeleteMessageBatchRequest();
        batchDelete.setQueueUrl(queue);
        batchDelete.setEntries(deleteRequests);

        sqs.deleteMessageBatch(batchDelete);
    }
    
    private List<Message> pull(String queue, int max) {
        ReceiveMessageResult result = sqs.receiveMessage(new ReceiveMessageRequest(queue).withMaxNumberOfMessages(max));
        
        return result.getMessages();
    }

    /**
     * Translate the SQS queue name to the fully qualified URL.
     * 
     * @param name
     * @return 
     */
    public String resolveQueueUrl(String name) {
        return sqs.getQueueUrl(new GetQueueUrlRequest(name)).getQueueUrl();
    }
    
    public String getConsistencyQueue() {
        return consistencyQueue;
    }

    public String getTimeoutQueue() {
        return timeoutQueue;
    }
    
    
    public static void main(String[] args) throws Exception {
        AlertJanitor janitor = new AlertJanitor();
        janitor.initalize(new URI("s3n://placeholder"), new Configuration());
        
        if("clearAll".equalsIgnoreCase(args[0])) {
            janitor.clearAll(janitor.consistencyQueue);
            janitor.clearAll(janitor.timeoutQueue);
        }
        
        if("log".equalsIgnoreCase(args[0])) {
            String queue = args[1];
            Path path = new Path(args[2]);
            
            if("timeout".equals(queue)) {
                janitor.writeLogs(janitor.timeoutQueue, path);
            } 
            
            if("alert".equals(queue)) {
                janitor.writeLogs(janitor.consistencyQueue, path);
            }
            
        }
    }
    
    
}
