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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Removes any entries from the Metastore that do not currently list in the
 * file system.
 * 
 * @author dweeks
 */
public class MetastoreResolveCommand extends Command {

    @Override
    public void execute(Configuration conf, String[] args) throws Exception {
        try {
            
            Path path = new Path(args[0]);
            
            conf.set("s3mper.metastore.deleteMarker.enabled", "true");
            
            FileSystemMetastore meta = Metastore.getFilesystemMetastore();
            meta.initalize(path.toUri(), conf);
            
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            
            Set<String> s3files = new HashSet<String>();
            
            FileStatus[] s3listing = fs.listStatus(path);
            
            if (s3listing != null) {
                for (FileStatus f : s3listing) {
                    s3files.add(f.getPath().toUri().toString());
                }
            }
            
            List<FileInfo> files = meta.list(Collections.singletonList(path));
            
            for (FileInfo f : files) {
                if (!s3files.contains(f.getPath().toUri().toString())) {
                    meta.delete(f.getPath());
                }
            }            
        } catch (Exception e) {
            System.out.println("Usage: s3mper metastore resolve <path>\n");
            
            e.printStackTrace();
        }
    }
    
}
