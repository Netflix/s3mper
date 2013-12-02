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


package com.netflix.bdp.s3mper.common;

import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

import static java.lang.String.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * A callable task that will automatically retry on failure.  The retry task is
 * callable itself, so it is possible to use with an executor or just invoke
 * the call() method.
 * 
 * @param <T> 
 * @author dweeks
 */
public class RetryTask<T> implements Callable<T> {
    private static final Logger log = Logger.getLogger(RetryTask.class.getName());
    
    private int maxRetries = 10;
    private int timeout = 1000;
    
    private BackoffAlgorithm backoff = new ExponentialBackoffAlgorithm(1000);
    
    private Callable<T> target;
    
    public RetryTask(Callable<T> target) {
        this.target = target;
    }

    public RetryTask(Callable<T> target, int maxRetries, int timeout) {
        this.target = target;
        this.maxRetries = maxRetries;
        this.timeout = timeout;
    }

    public RetryTask(Callable<T> target, int maxRetries, int timeout, BackoffAlgorithm backoff) {
        this.target = target;
        this.maxRetries = maxRetries;
        this.timeout = timeout;
        this.backoff = backoff;
    }
    
    @Override
    public T call() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        try {
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    FutureTask<T> future = new FutureTask<T>(target);

                    executor.submit(future);

                    return future.get(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (CancellationException ce) {
                    throw ce;
                } catch (Exception e) {
                    log.warn(format("Call attempt failed (%d of %d)", attempt, maxRetries));

                    if(attempt == maxRetries) {
                        throw e;
                    }

                    Thread.sleep(backoff.next());
                }
            }
        } finally {
            executor.shutdown();
        }
        
        throw new RuntimeException("Unexpected retry call failure");
    }
    
}
