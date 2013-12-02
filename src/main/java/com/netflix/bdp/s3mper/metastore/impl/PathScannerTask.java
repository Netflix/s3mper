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
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;

/**
 * Scans entries by path in DynamoDb using the epoch field to determine their age and places
 * old entries into a queue.
 * 
 * @author dweeks
 */
public class PathScannerTask extends AbstractScannerTask {
    private static final Logger log = Logger.getLogger(PathScannerTask.class.getName());
    
    private final BlockingQueue<Key> deleteQueue;
    private long age;
    private int queueSize;
    
    public PathScannerTask(AmazonDynamoDBClient db, RateLimiter limiter, BlockingQueue<Key> deleteQueue, int queueSize, long age) {
        super(db, limiter);
        this.deleteQueue = deleteQueue;
        this.age = age;
        this.queueSize = queueSize;
    }

    @Override
    public Object call() throws Exception {
        running = true;
        
        long deleteEpoch = System.currentTimeMillis() - age;
        
        Map<String, Condition> filter = new HashMap<String, Condition>();
        
        AttributeValue value = new AttributeValue();
        value.setN(deleteEpoch + "");
        
        Condition c = new Condition();
        c.setComparisonOperator(ComparisonOperator.LT);
        c.setAttributeValueList(Collections.singletonList(value));
        
        filter.put("epoch", c);
        
        ScanRequest scan = new ScanRequest(MetastoreJanitor.tableName);
        scan.setScanFilter(filter);
        scan.setLimit( (int) limiter.getRate());
        
        ScanResult result;
        
        int scanTotal = 0;
        int matched = 0;
        
        do {
            //Can't set a hard limit on the queue since paths can be resubmitted by delete task
            synchronized (deleteQueue) {
                while (deleteQueue.size() >= queueSize) {
                    deleteQueue.wait();
                }
            }
            
            if(!running) {
                break;
            }
            
            result = db.scan(scan);
            scanTotal += result.getScannedCount();
            matched += result.getCount();
            
            log.info(String.format("Total scanned: %d, matched: %d, added: %d, queue size: %d, consumed capacity: %f, max rate: %f", scanTotal, matched, result.getCount(), deleteQueue.size(), result.getConsumedCapacityUnits(), limiter.getRate()));
            
            for (Map<String, AttributeValue> i : result.getItems()) {
                if (!i.containsKey("epoch")) {
                    continue;
                }
                deleteQueue.put(new Key(i.get(DynamoDBMetastore.HASH_KEY), i.get(DynamoDBMetastore.RANGE_KEY)));
            }
            
            limiter.acquire(result.getConsumedCapacityUnits().intValue());
            
            scan.setExclusiveStartKey(result.getLastEvaluatedKey());
        } while (running && result.getLastEvaluatedKey() != null);
        
        return Boolean.TRUE;
    }
    
}
