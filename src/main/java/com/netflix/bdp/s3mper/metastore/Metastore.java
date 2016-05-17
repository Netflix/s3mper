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

package com.netflix.bdp.s3mper.metastore;

import com.netflix.bdp.s3mper.metastore.impl.BigTableMetastore;
import com.netflix.bdp.s3mper.metastore.impl.DynamoDBMetastore;
import com.netflix.bdp.s3mper.metastore.impl.InMemoryMetastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Provides basic file metadata for the metastore
 *
 * @author liljencrantz@spotify.com
 */
public class Metastore {

    private static final Logger log = Logger.getLogger(Metastore.class);

    private static FileSystemMetastore metastore;

    public static FileSystemMetastore getFilesystemMetastore(Configuration conf)
            throws Exception {
        if (metastore == null) {
            synchronized (Metastore.class) {
                if (metastore == null) {
                    Class<?> metaImpl = conf.getClass("s3mper.metastore.impl",
                                com.netflix.bdp.s3mper.metastore.impl.BigTableMetastore.class);
//                            com.netflix.bdp.s3mper.metastore.impl.DynamoDBMetastore.class);

                    try {
                        metastore = (FileSystemMetastore) ReflectionUtils.newInstance(metaImpl, conf);
                    } catch (Exception e) {
                        log.error("Error initializing s3mper metastore", e);
                        throw e;
                    }

                }
            }
        }
        return metastore;
    }

}
