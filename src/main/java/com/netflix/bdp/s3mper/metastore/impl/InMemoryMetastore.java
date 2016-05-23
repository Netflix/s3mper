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

import com.google.common.collect.ImmutableList;

import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.*;

/**
 * In memory implementation of the FileSystemMetastore interface,
 * only useful for testing purposes.
 *
 * @author liljencrantz@spotify.com
 */
public class InMemoryMetastore implements FileSystemMetastore {
    private static final Logger log = Logger.getLogger(InMemoryMetastore.class.getName());

    private Map<Path, List<FileInfo>> data;

    @Override
    public void initalize(URI uri, Configuration conf) throws Exception {
        data = new HashMap<Path, List<FileInfo>>();
    }

    @Override
    public List<FileInfo> list(List<Path> pathList) throws Exception {
        synchronized (this) {
            ImmutableList.Builder<FileInfo> result = new ImmutableList.Builder<FileInfo>();
            for (Path path : pathList) {
                result.addAll(get(path));
            }
            return result.build();
        }
    }

    @Override
    public void add(List<FileInfo> path) throws Exception {
        MetastoreFallback.add(this, path);
    }

    @Override
    public void add(Path path, boolean directory) throws Exception {
        synchronized (this) {
            delete(path);
            get(path.getParent()).add(new FileInfo(path, false, directory));
        }
    }

    @Override
    public void delete(final Path path) throws Exception {
        synchronized (this) {
            List<FileInfo> list = get(path.getParent());
            for (Iterator<FileInfo> it = list.iterator(); it.hasNext();) {
                FileInfo fi = it.next();
                if (fi.getPath().equals(path)) {
                    it.remove();
                }
            }
        }
    }

    @Override
    public void delete(List<Path> path) throws Exception {
        MetastoreFallback.delete(this, path);
    }

    @Override
    public void close() {
        data = null;
    }

    @Override
    public int getTimeout() {
        return 0;
    }

    @Override
    public void setTimeout(int timeout) {
    }

    private List<FileInfo> get(Path path) {
        if (!data.containsKey(path)) {
            data.put(path, new ArrayList<FileInfo>());
        }
        return data.get(path);
    }

}
