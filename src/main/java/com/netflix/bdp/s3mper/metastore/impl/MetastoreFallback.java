package com.netflix.bdp.s3mper.metastore.impl;

import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Utility class that provides fallback implementations of batch API calls
 *
 * @author dweeks
 */
public class MetastoreFallback {

    static void add(FileSystemMetastore metastore, List<FileInfo> path) throws Exception {
        for (FileInfo file: path) {
            metastore.add(file.getPath(), file.isDirectory());
        }
    }

    static void delete(FileSystemMetastore metastore, List<Path> path) throws Exception {
        for (Path file: path) {
            metastore.delete(file);
        }
    }
}
