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
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.Callable;

/**
 * Common ancestor of all DynamoDB operations.
 * 
 * @author dweeks
 */
public abstract class AbstractDynamoDBTask implements Callable {
    protected AmazonDynamoDBClient db;
    protected RateLimiter limiter;
    protected volatile boolean running = false;

    public AbstractDynamoDBTask(AmazonDynamoDBClient db, RateLimiter limiter) {
        this.db = db;
        this.limiter = limiter;
    }

    public AmazonDynamoDBClient getDb() {
        return db;
    }

    public void setDb(AmazonDynamoDBClient db) {
        this.db = db;
    }

    public RateLimiter getLimiter() {
        return limiter;
    }

    public void setLimiter(RateLimiter limiter) {
        this.limiter = limiter;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
    
}
