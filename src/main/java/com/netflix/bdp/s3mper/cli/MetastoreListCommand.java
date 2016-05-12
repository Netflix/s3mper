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


package com.netflix.bdp.s3mper.cli;

import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import com.netflix.bdp.s3mper.metastore.Metastore;
import com.netflix.bdp.s3mper.metastore.impl.DynamoDBMetastore;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author dweeks
 */
public class MetastoreListCommand extends Command {

    @Override
    public void execute(Configuration conf, String[] args) throws Exception {
        try {
            
            Path path = new Path(args[0]);
            
            conf.set("s3mper.metastore.deleteMarker.enabled", "true");
            
            FileSystemMetastore meta = Metastore.getFilesystemMetastore();
            meta.initalize(path.toUri(), conf);
            
            List<FileInfo> files = meta.list(Collections.singletonList(path));
            
            for (FileInfo f : files) {
                System.out.println(f.getPath() + (f.isDeleted() ? " [Deleted]" : ""));
            }
        } catch (Exception e) {
            System.out.println("Usage: s3mper metastore list <path>\n");
            
            e.printStackTrace();
        }
    }
    
}
