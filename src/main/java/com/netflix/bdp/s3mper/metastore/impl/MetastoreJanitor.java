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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.Key;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.*;

/**
 * Class to cleanup old entries in the DynamoDb Metastore.  This is intended to be a single use class.
 * It is not thread safe and should not be reused.
 * 
 * @author dweeks
 */
public class MetastoreJanitor {
    private static final Logger log = Logger.getLogger(MetastoreJanitor.class.getName());
    
    static String tableName = "ConsistentListingMetastore";
    private DynamoDBMetastore metastore = null;
    private AmazonDynamoDBClient db = null;
    
    private int scanLimit = Integer.getInteger("s3mper.janitor.scan.limit", 500);
    private int deleteLimit = Integer.getInteger("s3mper.janitor.delete.limit", 500);
    private int queueSize = Integer.getInteger("s3mper.janitor.queue.limit", 2000);
    private int scanThreads = Integer.getInteger("s3mper.janitor.threads.scan", 1);
    private int deleteThreads = Integer.getInteger("s3mper.janitor.threads.delete", 10);
    
    private RateLimiter scanLimiter;
    private RateLimiter deleteLimiter;
    
    private ExecutorService executor;
    private final List<AbstractDynamoDBTask> tasks = Collections.synchronizedList(new ArrayList<AbstractDynamoDBTask>());
    private final List<Future> futures = Collections.synchronizedList(new ArrayList<Future>());
    private final List<Future> scanFutures = Collections.synchronizedList(new ArrayList<Future>());
    
    private BlockingQueue<Key> queue = new LinkedBlockingQueue<Key>();
    
    public void initalize(URI uri, Configuration conf) throws Exception {
        String keyId = conf.get("fs."+uri.getScheme()+".awsAccessKeyId");
        String keySecret = conf.get("fs."+uri.getScheme()+".awsSecretAccessKey");
        
        //An override option for accessing across accounts
        keyId = conf.get("s3mper.override.awsAccessKeyId", keyId);
        keySecret = conf.get("s3mper.override.awsSecretAccessKey", keySecret);
        
        db = new AmazonDynamoDBClient(new BasicAWSCredentials(keyId, keySecret));
        
        tableName = conf.get("s3mper.metastore.name", tableName);
        
        metastore = new DynamoDBMetastore();
        metastore.initalize(uri, conf);
    }
    
    /**
     * Deletes all entries for a given path (directory) from the metastore.
     * 
     * @param path
     * @throws Exception 
     */
    public void clearPath(Path path) throws Exception {
        List<FileInfo> listing = metastore.list(Collections.singletonList(path), true);
        
        for(FileInfo file : listing) {
            metastore.delete(file.getPath());
        }
    }
    
    /**
     * Scans the timeseries index in dynamodb (i.e. hash key = 'epoch' ) and 
     * deletes entries older than the given time.
     * 
     * @param unit
     * @param time
     * @throws Exception 
     */
    public void deleteTimeseries(TimeUnit unit, long time) throws Exception {
        log.info("Starting Timeseries Delete");
        log.info(format("read_units=%d, write_units=%d, queue_size=%d, scan_threads=%d, delete_threads=%d", scanLimit, deleteLimit, queueSize, scanThreads, deleteThreads));
        
        executor = Executors.newFixedThreadPool(scanThreads+deleteThreads);
        
        for (int i = 0; i < scanThreads; i++) {
            TimeseriesScannerTask scanner = new TimeseriesScannerTask(db, scanLimiter, queue, queueSize, unit.toMillis(time));
            
            tasks.add(scanner);
            
            Future scanFuture = executor.submit(scanner);
            
            futures.add(scanFuture);
            scanFutures.add(scanFuture);
        }
        
        processDelete();
    }
    
    /**
     * Delete paths entries older than the time period provided. This requires
     * a full scan of the table, which is very resource intensive, so timeseries
     * is the preferred approach for deleting entries.
     * 
     * @param unit
     * @param time
     * @throws Exception  
     */
    public void deletePaths(TimeUnit unit, long time) throws Exception {
        log.info("Starting Full Path Delete");
        log.info(format("read_units=%d, write_units=%d, queue_size=%d, scan_threads=%d, delete_threads=%d", scanLimit, deleteLimit, queueSize, scanThreads, deleteThreads));
        
        executor = Executors.newFixedThreadPool(scanThreads+deleteThreads);
        
        log.info(format("Scanning for items older than: %d (ms)", unit.toMillis(time)));
        
        for (int i = 0; i < scanThreads; i++) {
            PathScannerTask scanner = new PathScannerTask(db, scanLimiter, queue, queueSize, unit.toMillis(time));
            
            tasks.add(scanner);
            Future scanFuture = executor.submit(scanner);
            
            futures.add(scanFuture);
            scanFutures.add(scanFuture);
        }
        
        processDelete();
    }
    
    private void processDelete() throws Exception {
        registerShutdownHook();
        
        for (int i = 0; i < deleteThreads; i++) {
            DeleteWriterTask delete = new DeleteWriterTask(db, deleteLimiter, queue);
            
            tasks.add(delete);
            futures.add(executor.submit(delete));
        }
        
        synchronized(scanFutures) {
            for (Future future : scanFutures) {
                future.get();
            }
        }
        
        synchronized(tasks) {
            for (AbstractDynamoDBTask task : tasks) {
                task.running = false;
            }
        }
       
        log.info("Shutting down . . .");
        executor.shutdown();
        log.info("Shutdown complete.");
    }
    
    /**
     * Attempts to shutdown cleanly by finishing processing for all entries in 
     * the queue.  If not done cleanly, some entries timeseries entries may get
     * deleted without deleting their corresponding path entries. 
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread("Metastore Janitor Shutdown Hook"){

            @Override
            public void run() {
                log.info("Shutting down all threads");
                
                synchronized(tasks) {
                    for(AbstractDynamoDBTask task : tasks) {
                        task.running = false;
                    }
                }
                
                synchronized(futures) {
                    for(Future future: futures) {
                        try {
                            future.get();
                        } catch (Exception ex) {
                            log.error("",ex);
                        }
                    }
                }
                
                executor.shutdown();
            }
            
        });
    }

    public int getScanLimit() {
        return scanLimit;
    }

    public void setScanLimit(int scanLimit) {
        this.scanLimit = scanLimit;
        
        scanLimiter = RateLimiter.create(scanLimit);
    }

    public int getDeleteLimit() {
        return deleteLimit;
    }

    public void setDeleteLimit(int deleteLimit) {
        this.deleteLimit = deleteLimit;
        
        deleteLimiter = RateLimiter.create(deleteLimit);
    }

    public int getScanThreads() {
        return scanThreads;
    }

    public void setScanThreads(int scanThreads) {
        this.scanThreads = scanThreads;
    }

    public int getDeleteThreads() {
        return deleteThreads;
    }

    public void setDeleteThreads(int deleteThreads) {
        this.deleteThreads = deleteThreads;
    }

}
