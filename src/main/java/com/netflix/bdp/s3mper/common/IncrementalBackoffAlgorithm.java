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

import java.util.Iterator;

/**
 * Provides incremental backoff based on given increment.
 * 
 * @author dweeks
 */
public class IncrementalBackoffAlgorithm implements BackoffAlgorithm {
    
    private long increment;

    public IncrementalBackoffAlgorithm(long increment) {
        this.increment = increment;
    }
    
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Long next() {
        return increment;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
    }

}
