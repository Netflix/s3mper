package com.netflix.bdp.s3mper.metastore.impl;

import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.List;

public class LoggingMetastore implements FileSystemMetastore {
  private static final Logger log = Logger.getLogger(LoggingMetastore.class);
  private final FileSystemMetastore wrapped;

  public LoggingMetastore(FileSystemMetastore wrapped) {
    log.debug("Constructing metastore");
    this.wrapped = wrapped;
  }

  @Override
  public void initalize(URI uri, Configuration conf) throws Exception {
    log.debug("Initializing metastore");
    wrapped.initalize(uri, conf);
  }

  @Override
  public List<FileInfo> list(List<Path> paths) throws Exception {
    for (Path path : paths) {
      log.debug("Listing metastore entries for: " + path.toUri());
    }
    List<FileInfo> infos = wrapped.list(paths);
    for (FileInfo info : infos) {
      log.debug("Listing metastore result: " + info.getPath().toUri());
    }
    return infos;
  }

  @Override
  public void add(List<FileInfo> paths) throws Exception {
    for (FileInfo info : paths) {
      log.debug("Adding metastore entry for: " + info.getPath().toUri());
    }
    wrapped.add(paths);
  }

  @Override
  public void add(Path path, boolean directory) throws Exception {
    log.debug("Adding metastore entry for: " + path.toUri());
    wrapped.add(path, directory);
  }

  @Override
  public void delete(Path path) throws Exception {
    log.debug("Deleting metastore entry for: " + path.toUri());
    wrapped.delete(path);
  }

  @Override
  public void delete(List<Path> paths) throws Exception {
    for (Path path : paths) {
      log.debug("Adding metastore entry for: " + path.toUri());
    }
    wrapped.delete(paths);
  }

  @Override
  public void close() {
    log.debug("Closing metastore");
    wrapped.close();
  }

  @Override
  public int getTimeout() {
    int timeout = wrapped.getTimeout();
    log.debug("Getting timeout: " + timeout);
    return timeout;
  }

  @Override
  public void setTimeout(int timeout) {
    log.debug("Setting timeout to: " + timeout);
    wrapped.setTimeout(timeout);
  }
}
