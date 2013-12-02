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

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Utility for basic path information/operations.
 * 
 * @author dweeks
 */
public class PathUtil {
    public static final URI S3N;
    
    static {
        Configuration conf = new Configuration();
        String scheme = conf.get("s3mper.uri.scheme", System.getProperty("s3mper.uri.scheme", "s3n"));
        
        S3N = new Path(scheme+"://default").toUri();
    }
    
    public static String normalize(Path path) {
        return path.toUri().normalize().getSchemeSpecificPart().replaceAll("/$", "");
    }
    
    
}
