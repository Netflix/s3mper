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

import com.google.common.annotations.VisibleForTesting;

import com.netflix.bdp.s3mper.common.RetryTask;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ReturnValue;
import com.amazonaws.services.dynamodb.model.ScalarAttributeType;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

import static com.netflix.bdp.s3mper.common.PathUtil.*;
import java.util.Random;

/**
 * Implements FileSystemMetastore using DynamoDB as a backend.
 * 
 * @author dweeks
 */
@SuppressWarnings("deprecation")
public class DynamoDBMetastore implements FileSystemMetastore {
    private static final Logger log = Logger.getLogger(DynamoDBMetastore.class.getName());
    
    private String tableName = "ConsistentListingMetastore";
    private AmazonDynamoDBClient db = null;
    
    private long readUnits = 500;
    private long writeUnits = 100;
    
    private int retryCount = Integer.getInteger("s3mper.metastore.retry", 3);
    private int timeout = Integer.getInteger("s3mper.metastore.timeout", 5000);    
    private String scheme;
    
    private boolean deleteMarkerEnabled;
    
    private Random rand = new Random();
    
    static final String HASH_KEY = "path";
    static final String RANGE_KEY = "file";
    static final String EPOCH_VALUE = "epoch";
    static final String DIRECTORY_VALUE = "dir";
    static final String DELETE_MARKER = "deleted";
            
    static final String LINK_HASH_KEY = "linkPath";
    static final String LINK_RANGE_KEY = "linkFile";
    static final String TIMESERIES_KEY = "epoch";

    /**
     * Creates the metastore table in DynamoDB if it doesn't exist with the configured
     * read and write units.
     * 
     * @param uri
     * @param conf
     * @throws Exception 
     */
    @Override
    public void initalize(URI uri, Configuration conf) throws Exception {
        scheme = uri.getScheme();
        
        String keyId = conf.get("fs."+uri.getScheme()+".awsAccessKeyId");
        String keySecret = conf.get("fs."+uri.getScheme()+".awsSecretAccessKey");
        
        //An override option for accessing across accounts
        keyId = conf.get("s3mper.override.awsAccessKeyId", keyId);
        keySecret = conf.get("s3mper.override.awsSecretAccessKey", keySecret);
        
        db = new AmazonDynamoDBClient(new BasicAWSCredentials(keyId, keySecret));
        
        readUnits = conf.getLong("s3mper.metastore.read.units", readUnits);
        writeUnits = conf.getLong("s3mper.metastore.write.units", writeUnits);
        
        retryCount = conf.getInt("s3mper.metastore.retry", retryCount);
        timeout = conf.getInt("s3mper.metastore.timeout", timeout);
        
        tableName = conf.get("s3mper.metastore.name", tableName);
        
        deleteMarkerEnabled = conf.getBoolean("s3mper.metastore.deleteMarker.enabled", false);
        
        boolean checkTableExists = conf.getBoolean("s3mper.metastore.create", false);
        
        if(checkTableExists) {
            ListTablesResult tables = db.listTables();
            
            if(!tables.getTableNames().contains(tableName)) {
                createTable();
            }
        }
    }

    /**
     * Creates the table in DynamoDB. The hash+range key is:
     * 
     *           Hash (String)        |    Range (String)
     *             File Path                 File Name
     * Example:  s3n://netflix/data           test.xml
     *   
     * The table also includes one attribute for epoch (time created), but
     * that is only defined during the put operation (see add()).
     *
     */
    private void createTable() {
        log.info("Creating table in DynamoDB: " + tableName);
        
        CreateTableRequest createRequest = new CreateTableRequest();
        createRequest.withTableName(tableName);
        
        //Key
        KeySchemaElement pathKey = new KeySchemaElement().withAttributeName(HASH_KEY).withAttributeType(ScalarAttributeType.S);
        KeySchemaElement fileKey = new KeySchemaElement().withAttributeName(RANGE_KEY).withAttributeType(ScalarAttributeType.S);
        KeySchema schema = new KeySchema();
        schema.setHashKeyElement(pathKey);
        schema.setRangeKeyElement(fileKey);
        createRequest.setKeySchema(schema);
        
        //Throughput
        ProvisionedThroughput tp = new ProvisionedThroughput();
        tp.setReadCapacityUnits(readUnits);
        tp.setWriteCapacityUnits(writeUnits);
        
        createRequest.setProvisionedThroughput(tp);
        
        db.createTable(createRequest);
    }
    
    
    /**
     * Returns a list of files that should exist in the FileSystem.
     * 
     * @param paths
     * @return 
     * @throws java.lang.Exception 
     */
    @Override
    public List<FileInfo> list(List<Path> paths) throws Exception {
        return list(paths, deleteMarkerEnabled);
    }
    
    /**
     * Returns a list of files that should exist in the FileSystem with 
     * optional inclusion of deleted entries.
     * 
     * @param paths
     * @param includeDeleted
     * @return
     * @throws Exception 
     */
    public List<FileInfo> list(List<Path> paths, boolean includeDeleted) throws Exception {
        List<FileInfo> listing = new ArrayList<FileInfo>();
        
        for(Path path : paths) {
            Key startKey = null;
            
            do {
                RetryTask<QueryResult> queryTask = new RetryTask(new QueryTask(path, startKey), retryCount, timeout);
                QueryResult result = queryTask.call();
                
                for(Map<String, AttributeValue> item : result.getItems()) {
                    FileInfo file = new FileInfo(new Path(scheme+":"+item.get(HASH_KEY).getS() +"/"+ item.get(RANGE_KEY).getS()));
                    
                    if(item.containsKey(DELETE_MARKER)) {
                        file.setDeleted(Boolean.parseBoolean(item.get(DELETE_MARKER).getS()));
                        
                        //@TODO: cleanup deleteMarker logic after deployed
                        if(!includeDeleted) {
                            continue;
                        }
                    }
                    
                    if(item.containsKey(DIRECTORY_VALUE)) {
                        file.setDirectory(Boolean.parseBoolean((item.get(DIRECTORY_VALUE).getS())));
                    }
                    
                    listing.add(file);
                }
                
                startKey = result.getLastEvaluatedKey();
            } while(startKey != null);
        }
        
        return listing;
    }
    
    /**
     * Adds a path to the metastore.
     * 
     * @param path 
     * @throws java.lang.Exception 
     */
    @Override
    public void add(final Path path, boolean directory) throws Exception {
        RetryTask task = new RetryTask(new AddTask(path, directory), retryCount, timeout);
        
        task.call();
    }

    /**
     * Delete the provided path from the Metastore or use a delete marker.
     * 
     * @param path
     * @param deleteMarker 
     * @throws Exception 
     */
    @Override
    public void delete(final Path path) throws Exception {
        RetryTask task;
        
        if(deleteMarkerEnabled) {
            task = new RetryTask(new MarkDeletedTask(path), retryCount, timeout);
        } else {
            task = new RetryTask(new DeleteTask(path), retryCount, timeout);
        }
        
        task.call();
    }
    
    @Override
    public void close() {
    }
    
    /**
     * A Callable task for use with RetryTask to add a path to the
     * DynamoDB table.
     * 
     */
    private class AddTask implements Callable<Object> {
        private Path path;
        private boolean directory;
        
        public AddTask(Path path, boolean directory) {
            this.path = path;
            this.directory = directory;
        }
        
        @Override
        public Object call() throws Exception {
            long epoch = System.currentTimeMillis();
            
            AttributeValue avPath = new AttributeValue(normalize(path.getParent()));
            AttributeValue avFile = new AttributeValue(path.getName());
            AttributeValue avEpoch = new AttributeValue().withN(epoch+"");
            
            PutItemRequest put = new PutItemRequest();
            put.setTableName(tableName);
            Map<String, AttributeValue> items = new HashMap<String, AttributeValue>();

            items.put(HASH_KEY, avPath);
            items.put(RANGE_KEY, avFile);
            items.put(EPOCH_VALUE, avEpoch);
            
            if(directory) {
                items.put(DIRECTORY_VALUE, new AttributeValue(Boolean.TRUE.toString()));
            }
            
            put.setItem(items);

            if(log.isDebugEnabled()) {
                log.debug("Adding metastore entry for: " + path.toUri());
            }

            db.putItem(put);

            PutItemRequest tsPut = new PutItemRequest();
            tsPut.setTableName(tableName);
            Map<String, AttributeValue> tsItems = new HashMap<String, AttributeValue>();
            
            tsItems.put(HASH_KEY, new AttributeValue(TIMESERIES_KEY));
            tsItems.put(RANGE_KEY, new AttributeValue(epoch+"-"+rand.nextInt()));
            tsItems.put(LINK_HASH_KEY, avPath);
            tsItems.put(LINK_RANGE_KEY, avFile);
            tsPut.setItem(tsItems);
            
            db.putItem(tsPut);
            
            return null;
        }
        
    }
    
    /**
     * A Callable task to be used with RetryTask to query a path.
     * 
     * Takes a path and a scan start key and returns the query result.
     */
    class QueryTask implements Callable<QueryResult> {
        private Path path;
        private Key startKey;

        public QueryTask(Path path, Key startKey) {
            this.path = path;
            this.startKey = startKey;
        }
        
        @Override
        public QueryResult call() throws Exception {
            QueryRequest query = new QueryRequest();
            query.setTableName(tableName);
            query.withHashKeyValue(new AttributeValue(normalize(path)));
            query.setConsistentRead(true);

            if(startKey != null) {
                query.setExclusiveStartKey(startKey);
            }

            if(log.isDebugEnabled()) {
                log.debug("Querying DynamoDB for path: " + path.toUri());
            }
            
            return db.query(query);
        }
        
    }
    
    /**
     * A Callable task to be used with RetryTask to delete a file.
     */
    class DeleteTask implements Callable<DeleteItemResult> {
        private Path path;

        public DeleteTask(Path path) {
            this.path = path;
        }

        @Override
        public DeleteItemResult call() throws Exception {
            DeleteItemRequest delete = new DeleteItemRequest();
            delete.setTableName(tableName);
            delete.setKey(new Key(new AttributeValue(normalize(path.getParent())), new AttributeValue(path.getName())));
            delete.setReturnValues(ReturnValue.NONE);
            
            if(log.isDebugEnabled()) {
                log.debug("Deleting DynamoDB path: " + path.toUri());
            }
            
            return db.deleteItem(delete);
        }
        
    }
    
    /**
     * Marks a path deleted but does not actually delete the entry.
     */
    private class MarkDeletedTask implements Callable<UpdateItemResult> {
        private Path path;

        public MarkDeletedTask(Path path) {
            this.path = path;
        }
        
        @Override
        public UpdateItemResult call() throws Exception {
            UpdateItemRequest update = new UpdateItemRequest();
            update.setTableName(tableName);
            update.setKey(new Key(new AttributeValue(normalize(path.getParent())), new AttributeValue(path.getName())));
            
            Map<String, AttributeValueUpdate> items = new HashMap<String, AttributeValueUpdate>();
            items.put(DELETE_MARKER, new AttributeValueUpdate().withValue(new AttributeValue().withS(Boolean.TRUE.toString())));
            items.put(EPOCH_VALUE, new AttributeValueUpdate().withValue(new AttributeValue().withN(System.currentTimeMillis()+"")));
            
            update.setAttributeUpdates(items);

            if(log.isDebugEnabled()) {
                log.debug("Marking DynamoDB path deleted: " + path.toUri());
            }
            
            return db.updateItem(update);
        }
        
    }
  
    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
}
