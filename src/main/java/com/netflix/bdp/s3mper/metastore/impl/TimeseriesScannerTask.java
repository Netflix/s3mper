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


package com.netflix.bdp.s3mper.metastore.impl;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;

import static java.lang.String.*;
import java.util.Date;

/**
 * Scans the DynamoDB table using the timeseries index and pushes the entries into a queue.
 * 
 * @author dweeks
 */
public class TimeseriesScannerTask extends AbstractScannerTask {
    private static final Logger log = Logger.getLogger(TimeseriesScannerTask.class.getName());
    private final BlockingQueue<Key> deleteQueue;
    private int queueSize;
    private long age;
    
    private int reportInterval = 100000;

    public TimeseriesScannerTask(AmazonDynamoDBClient db, RateLimiter limiter, BlockingQueue<Key> deleteQueue, int queueSize, long age) {
        super(db, limiter);
        this.deleteQueue = deleteQueue;
        this.queueSize = queueSize;
        this.age = age;
    }

    @Override
    public Object call() throws Exception {
        running = true;
        
        long deleteEpoch = System.currentTimeMillis() - age;
        
        QueryRequest query = new QueryRequest();
        query.setTableName(MetastoreJanitor.tableName);
        query.setHashKeyValue(new AttributeValue().withS(DynamoDBMetastore.TIMESERIES_KEY));
        query.setLimit(queueSize/2);
        
        QueryResult result;
        
        int scanCount = 0;
        int deleteCount = 0;
        
        do {
            //Can't set a hard limit on the queue since paths can be resubmitted by delete task
            //which can cause a deadlock.
            synchronized (deleteQueue) {
                while (deleteQueue.size() >= queueSize) {
                    deleteQueue.wait();
                }
            }
            
            if(!running) {
                break;
            }
                
            result = db.query(query);
            
            scanCount += result.getCount();
            
            long epoch = deleteEpoch;
            
            for (Map<String, AttributeValue> i : result.getItems()) {
                epoch = Long.parseLong(i.get(DynamoDBMetastore.RANGE_KEY).getS().split("-")[0]);
                
                if (epoch >= deleteEpoch) {
                    log.info("Timeseries scan complete.  Exiting.");
                    running = false;
                    break;
                }
                
                deleteCount += 2;
                
                deleteQueue.put(new Key(i.get(DynamoDBMetastore.HASH_KEY), i.get(DynamoDBMetastore.RANGE_KEY)));
                deleteQueue.put(new Key(i.get(DynamoDBMetastore.LINK_HASH_KEY), i.get(DynamoDBMetastore.LINK_RANGE_KEY)));
            }
            
            if(scanCount % reportInterval == 0) {
                log.info(format("scanned: %d, added: %d, queue_size: %d, current_date: %s", scanCount, deleteCount, deleteQueue.size(), new Date(epoch)));
            }
            
            limiter.acquire(result.getConsumedCapacityUnits().intValue());
            
            query.setExclusiveStartKey(result.getLastEvaluatedKey());
        } while (running && result.getLastEvaluatedKey() != null);
        
        log.info(format("Scan Complete.%nEntries Scanned: %d%nEntries Deleted: %d", scanCount, deleteCount));
        
        return Boolean.TRUE;
    }
    
}
