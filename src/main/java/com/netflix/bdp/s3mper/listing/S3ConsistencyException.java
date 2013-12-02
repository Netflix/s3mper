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


package com.netflix.bdp.s3mper.listing;

/**
 * Exception that gets thrown if an inconsistent listing is detected.
 * 
 * @author dweeks
 */
public class S3ConsistencyException extends RuntimeException {

    public S3ConsistencyException() {
    }

    public S3ConsistencyException(String string) {
        super(string);
    }

    public S3ConsistencyException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public S3ConsistencyException(Throwable thrwbl) {
        super(thrwbl);
    }
    
}
