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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteResponse;
import com.amazonaws.services.dynamodb.model.DeleteRequest;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.WriteRequest;
import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

/**
 * Deletes entries on the queue from DynamoDb.
 * 
 * @author dweeks
 */
public class DeleteWriterTask extends AbstractDynamoDBTask {
    private static final Logger log = Logger.getLogger(DeleteWriterTask.class.getName());
    
    /** Batch limit is explicit from the AWS SDK */
    private final int batchLimit = 25;
    
    final BlockingQueue<Key> deleteQueue;

    public DeleteWriterTask(AmazonDynamoDBClient db, RateLimiter limiter, BlockingQueue<Key> deleteQueue) {
        super(db, limiter);
        this.deleteQueue = deleteQueue;
    }

    @Override
    public Object call() throws Exception {
        running = true;
        
        Set<Key> keys = new HashSet<Key>(batchLimit);
        List<WriteRequest> batch = new ArrayList<WriteRequest>(batchLimit);
        
        try {
            
            while (!deleteQueue.isEmpty() || running) {
                
                deleteQueue.drainTo(keys, batchLimit);
                
                synchronized (deleteQueue) {
                    deleteQueue.notifyAll();
                }
                
                if (keys.isEmpty()) {
                    Thread.sleep(500);
                    continue;
                }
                
                for (Key key : keys) {
                    batch.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(key)));
                }
                
                BatchWriteItemRequest batchRequest = new BatchWriteItemRequest();
                Map<String, List<WriteRequest>> itemRequests = new HashMap<String, List<WriteRequest>>();
                itemRequests.put(MetastoreJanitor.tableName, batch);
                batchRequest.setRequestItems(itemRequests);
                
                BatchWriteItemResult result = db.batchWriteItem(batchRequest);
                
                //Resubmit failed writes
                for (Map.Entry<String, List<WriteRequest>> e : result.getUnprocessedItems().entrySet()) {
                    for (WriteRequest w : e.getValue()) {
                        deleteQueue.put(w.getDeleteRequest().getKey());
                    }
                }
                
                //Drain capacity
                for (Map.Entry<String, BatchWriteResponse> e : result.getResponses().entrySet()) {
                    limiter.acquire(e.getValue().getConsumedCapacityUnits().intValue());
                }
                
                if(log.isDebugEnabled()) {
                    log.debug(String.format("delete: %2d, queue_size: %5d, max_rate: %4.1f", keys.size(), deleteQueue.size(), limiter.getRate()));
                }
                
                keys.clear();
                batch.clear();
            }
        } catch (InterruptedException interruptedException) {
            log.error("Interrupted", interruptedException);
        } catch (AmazonClientException amazonClientException) {
            log.error("", amazonClientException);
        }
        
        log.info("Delete task terminating");
        return Boolean.FALSE;
    }
    
}
