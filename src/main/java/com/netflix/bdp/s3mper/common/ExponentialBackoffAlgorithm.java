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

/**
 * Provides exponential backoff based on the provided initial delay.
 * 
 * @author dweeks
 */
public class ExponentialBackoffAlgorithm implements BackoffAlgorithm {
    public long delay;
    public int attempt = 0;
    
    public ExponentialBackoffAlgorithm(long delay) {
        this.delay = delay;
    }
    
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Long next() {
        return (long) (delay * Math.pow(2, attempt++));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() not supported");
    }
    
}
