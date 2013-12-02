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

import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import com.netflix.bdp.s3mper.alert.AlertDispatcher;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import static com.netflix.bdp.s3mper.common.PathUtil.*;
import static java.lang.String.*;
import java.util.Iterator;

/**
 * This class provides advice to the S3 hadoop FileSystem implementation and uses
 * a metastore for consistent listing.
 * 
 * @author dweeks
 */
@Aspect
public abstract class ConsistentListingAspect {
    private static final Logger log = Logger.getLogger(ConsistentListingAspect.class.getName());
    
    private FileSystemMetastore metastore = null;
    private AlertDispatcher alertDispatcher = null;
        
    private boolean disabled = true;
    private boolean failOnError = Boolean.getBoolean("s3mper.failOnError");
    private boolean taskFailOnError = Boolean.getBoolean("s3mper.task.failOnError");
    private boolean checkTaskListings = Boolean.getBoolean("s3mper.listing.task.check"); 
    private boolean failOnTimeout = Boolean.getBoolean("s3mper.failOnTimeout");
    private boolean trackDirectories = Boolean.getBoolean("s3mper.listing.directory.tracking");
    private boolean delistDeleteMarkedFiles = true;
    
    private float fileThreshold = 1;
    
    private long recheckCount = Long.getLong("s3mper.listing.recheck.count", 15);
    private long recheckPeriod = Long.getLong("s3mper.listing.recheck.period", TimeUnit.MINUTES.toMillis(1));
    private long taskRecheckCount = Long.getLong("s3mper.listing.task.recheck.count", 0);
    private long taskRecheckPeriod = Long.getLong("s3mper.listing.task.recheck.period", TimeUnit.MINUTES.toMillis(1));
    
    @Pointcut 
    public abstract void init();
    /**
     * Creates the metastore on initialization.
     * 
     * #TODO The metastore isn't created instantly by DynamoDB.  This should wait until
     * the initialization is complete.  If the store doesn't exist, calls will fail until
     * it is created.
     * 
     * @param jp
     * @throws Exception  
     */
    @Before("init()") 
    public synchronized void initialize(JoinPoint jp) throws Exception {
        URI uri = (URI) jp.getArgs()[0];
        Configuration conf = (Configuration) jp.getArgs()[1];
        
        updateConfig(conf);
        
        //Check again after updating configs
        if(disabled) {
            return;
        }
        
        if(metastore == null) {
            log.debug("Initializing S3mper Metastore");
            
            //FIXME: This is defaulted to the dynamodb metastore impl, but shouldn't 
            //       reference it directly like this.
            Class<?> metaImpl = conf.getClass("s3mper.metastore.impl", com.netflix.bdp.s3mper.metastore.impl.DynamoDBMetastore.class);

            try {
                metastore = (FileSystemMetastore) ReflectionUtils.newInstance(metaImpl, conf);
                metastore.initalize(uri, conf);
            } catch (Exception e) {
                log.error("Error initializing s3mper metastore", e);

                disable();

                if(failOnError) {
                    throw e;
                } 
            }
        } else {
            log.debug("S3mper Metastore already initialized.");
        }
        
        if(alertDispatcher == null) {
            log.debug("Initializing Alert Dispatcher");
            
            try {
                Class<?> dispatcherImpl = conf.getClass("s3mper.dispatcher.impl", com.netflix.bdp.s3mper.alert.impl.CloudWatchAlertDispatcher.class);
                
                alertDispatcher = (AlertDispatcher) ReflectionUtils.newInstance(dispatcherImpl, conf);
                alertDispatcher.init(uri, conf);
            } catch (Exception e) {
                log.error("Error initializing s3mper alert dispatcher", e);

                disable();

                if(failOnError) {
                    throw e;
                }
            }
        } else {
            alertDispatcher.setConfig(conf);
        }
    }
    
    private void updateConfig(Configuration conf) {
        disabled = conf.getBoolean("s3mper.disable", disabled);
        
        if(disabled) {
            log.warn("S3mper Consistency explicitly disabled.");
            return;
        }
        
        failOnError = conf.getBoolean("s3mper.failOnError", failOnError);
        taskFailOnError = conf.getBoolean("s3mper.task.failOnError", taskFailOnError);
        checkTaskListings = conf.getBoolean("s3mper.listing.task.check", checkTaskListings);
        failOnTimeout = conf.getBoolean("s3mper.failOnTimeout", failOnTimeout);
        delistDeleteMarkedFiles = conf.getBoolean("s3mper.listing.delist.deleted", delistDeleteMarkedFiles);
        trackDirectories = conf.getBoolean("s3mper.listing.directory.tracking", trackDirectories);
        
        fileThreshold = conf.getFloat("s3mper.listing.threshold", fileThreshold);
        
        recheckCount = conf.getLong("s3mper.listing.recheck.count", recheckCount);
        recheckPeriod = conf.getLong("s3mper.listing.recheck.period", recheckPeriod);
        taskRecheckCount = conf.getLong("s3mper.listing.task.recheck.count", taskRecheckCount);
        taskRecheckPeriod = conf.getLong("s3mper.listing.task.recheck.period", taskRecheckPeriod);
    }
    
    @Pointcut
    public abstract void create();
    
    /**
     * Updates the metastore when a FileSystem.create(...) method is called.
     * 
     * @param pjp
     * @return
     * @throws Throwable 
     */
    @Around("create() && !within(ConsistentListingAspect)")
    public Object metastoreUpdate(final ProceedingJoinPoint pjp) throws Throwable {
        if(disabled) {
            return pjp.proceed();
        }
        
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        updateConfig(conf);
        
        Object result = pjp.proceed();
        
        Path path = null;
        
        if (result instanceof Boolean && !((Boolean) result)) {
            return result;
        }
        
        try {
            //Locate the path parameter in the arguments
            for (Object arg : pjp.getArgs()) {
                if (arg instanceof Path) {
                    path = (Path) arg;
                    break;
                }
            }
            
            metastore.add(path, pjp.getSignature().getName().contains("mkdir"));
        } catch (TimeoutException t) {
            log.error("Timeout occurred adding path to metastore: " + path, t);
            
            alertDispatcher.timeout("metastoreUpdate", Collections.singletonList(path));
            
            if(failOnTimeout) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Failed to add path to metastore: " + path, e);
            
            if(shouldFail(conf)) {
                throw e;
            }
        }
        
        return result;
    }
    
    @Pointcut
    public abstract void list();
    
    /**
     * Ensures that all the entries in the metastore also exist in the FileSystem listing.
     * 
     * @param pjp
     * @return
     * @throws Throwable 
     */
    @Around("list() && !cflow(delete()) && !within(ConsistentListingAspect)")
    public Object metastoreCheck(final ProceedingJoinPoint pjp) throws Throwable {
        if(disabled) {
            return pjp.proceed();
        }
        
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        updateConfig(conf);
        
        FileStatus [] s3Listing = (FileStatus[]) pjp.proceed();
        
        List<Path> pathsToCheck = new ArrayList<Path>();
        
        Object pathArg = pjp.getArgs()[0];
        
        //Locate paths in the arguments
        if(pathArg instanceof Path) {
            pathsToCheck.add((Path)pathArg);
        } else if (pathArg instanceof List) {
            pathsToCheck.addAll((List)pathArg);
        } else if (pathArg.getClass().isArray()) {
            pathsToCheck.addAll(Arrays.asList((Path[]) pathArg));
        }
        
        //HACK: This is just to prevent the emr metrics from causing consisteny failures
        for(StackTraceElement e : Thread.currentThread().getStackTrace()) {
            if(e.getClassName().contains("emr.metrics")) {
                log.debug("Ignoring EMR metrics listing for paths: " + pathsToCheck);
                return s3Listing;
            }
        }
        //END HACK
        
        long recheck = recheckCount;
        long delay = recheckPeriod;
        
        try {
            if (isTask(conf) && !checkTaskListings) {
                log.info("Skipping consistency check for task listing");
                return s3Listing;
            }
            
            if(isTask(conf)) {
                recheck = taskRecheckCount;
                delay = taskRecheckPeriod;
            }
        } catch (Exception e) {
            log.error("Error checking for task side listing", e);
        }
        
        try {
            List<FileInfo> metastoreListing = metastore.list(pathsToCheck);
            
            List<Path> missingPaths = new ArrayList<Path>(0);
            
            int checkAttempt;
            
            for(checkAttempt=0; checkAttempt<=recheck; checkAttempt++) {
                missingPaths = checkListing(metastoreListing, s3Listing);
                
                if(delistDeleteMarkedFiles) {
                    s3Listing = delistDeletedPaths(metastoreListing, s3Listing);
                }
                
                if(missingPaths.isEmpty()) {
                    break;
                }
                
                //Check if acceptable threshold of data has been met.  This is a little
                //ambigious becuase S3 could potentially have more files than the
                //metastore (via out-of-band access) and throw off the ratio
                if(fileThreshold < 1 && metastoreListing.size() > 0) {
                    float ratio = s3Listing.length / (float) metastoreListing.size();
                    
                    if(ratio > fileThreshold) {
                        log.info(format("Proceeding with incomplete listing at ratio %f (%f as acceptable). Still missing paths: %s", ratio, fileThreshold, missingPaths));
                        
                        missingPaths.clear();
                        break;
                    }
                }
                
                if(recheck == 0) {
                    break;
                }
                
                log.info(format("Rechecking consistency in %d (ms).  Files missing %d. Missing paths: %s", delay, missingPaths.size(), missingPaths));
                Thread.sleep(delay);
                s3Listing = (FileStatus[]) pjp.proceed();
            }
            
            if (!missingPaths.isEmpty()) {
                alertDispatcher.alert(missingPaths);
                
                if (shouldFail(conf)) {
                    throw new S3ConsistencyException("Consistency check failed. See go/s3mper for details. Missing paths: " + missingPaths);
                } else {
                    log.error("Consistency check failed.  See go/s3mper for details. Missing paths: " + missingPaths);
                }
            } else {
                if(checkAttempt > 0) {
                    log.info(format("Listing achieved consistency after %d attempts", checkAttempt));
                    alertDispatcher.recovered(pathsToCheck);
                }
            }
        } catch (TimeoutException t) {
            log.error("Timeout occurred listing metastore paths: " + pathsToCheck, t);
            
            alertDispatcher.timeout("metastoreCheck", pathsToCheck);
            
            if(failOnTimeout) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Failed to list metastore for paths: " + pathsToCheck, e);
            
            if(shouldFail(conf)) {
                throw e;
            }
        }
        
        return s3Listing;
    }
    
    /**
     * Check the the metastore listing against the s3 listing and return any paths 
     * missing from s3.
     * 
     * @param metastoreListing
     * @param s3Listing
     * @return 
     */
    private List<Path> checkListing(List<FileInfo> metastoreListing, FileStatus [] s3Listing) {
        Map<String, FileStatus> s3paths = new HashMap<String, FileStatus>();
            
        if(s3Listing != null) {
            for (FileStatus fileStatus : s3Listing) {
                s3paths.put(fileStatus.getPath().toUri().normalize().getSchemeSpecificPart(), fileStatus);
            }
        }

        List<Path> missingPaths = new ArrayList<Path>();

        for (FileInfo f : metastoreListing) {
            if(f.isDeleted()) {
                continue;
            }
            
            if (!s3paths.containsKey(f.getPath().toUri().normalize().getSchemeSpecificPart())) {
                missingPaths.add(f.getPath());
            }
        }
        
        return missingPaths;
    }

    private FileStatus [] delistDeletedPaths(List<FileInfo> metastoreListing, FileStatus [] s3Listing) {
        if(s3Listing == null || s3Listing.length == 0) {
            return s3Listing;
        }
        
        Set<String> delistedPaths = new HashSet<String>();
        
        for(FileInfo file : metastoreListing) {
            if(file.isDeleted()) {
                delistedPaths.add(normalize(file.getPath()));
            }
        }
        
        List<FileStatus> s3files = Arrays.asList(s3Listing);
        
        for (Iterator<FileStatus> i = s3files.iterator(); i.hasNext();) {
            FileStatus file = i.next();
            
            if(delistedPaths.contains(normalize(file.getPath())) ) {
                i.remove();
            }
        }
            
        return s3files.toArray(new FileStatus[s3files.size()]);
    }
    
    
    @Pointcut
    public abstract void delete();
    
    /**
     * Deletes listing records based on a delete call from the FileSystem.
     * 
     * @param pjp
     * @return
     * @throws Throwable 
     */
    @Around("delete() && !within(ConsistentListingAspect)")
    public Object metastoreDelete(final ProceedingJoinPoint pjp) throws Throwable {
        if(disabled) {
            return pjp.proceed();
        }
        
        Configuration conf = ((FileSystem) pjp.getTarget()).getConf();
        updateConfig(conf);
        
        Path deletePath = (Path) pjp.getArgs()[0];
        
        boolean recursive = false;
            
        if(pjp.getArgs().length > 1) {
            recursive = (Boolean) pjp.getArgs()[1];
        }
            
        try {
            FileSystem s3fs = (FileSystem) pjp.getTarget();
            
            Set<Path> filesToDelete = new HashSet<Path>();
            filesToDelete.add(deletePath);
            
            List<FileInfo> metastoreFiles = metastore.list(Collections.singletonList(deletePath));
            
            for(FileInfo f : metastoreFiles) {
                filesToDelete.add(f.getPath());
            }
            
            try {
                if(s3fs.getFileStatus(deletePath).isDir() && recursive) {
                    filesToDelete.addAll(recursiveList(s3fs, deletePath));
                }
            } catch (Exception e) {
                log.info("A problem occurred deleting path: " + deletePath +" "+ e.getMessage());
            }
            
            for(Path path : filesToDelete) {
                metastore.delete(path);
            }
        } catch (TimeoutException t) {
            log.error("Timeout occurred deleting metastore path: " + deletePath, t);
            
            alertDispatcher.timeout("metastoreDelete", Collections.singletonList(deletePath));
            
            if(failOnTimeout) {
                throw t;
            }
        } catch (Exception e) {
            log.error("Error deleting paths from metastore: " + deletePath, e);
            
            if(shouldFail(conf)) {
                throw e;
            }
        }
        
        return pjp.proceed();
    }
    
    private List<Path> recursiveList(FileSystem fs, Path path) throws IOException {
        List<Path> result = new ArrayList<Path>();
        
        try {
            result.add(path);
            
            if (!fs.isFile(path)) {
                FileStatus[] children = fs.listStatus(path);
                
                if (children == null) {
                    return result;
                }
                
                for (FileStatus child : children) {
                    if (child.isDir()) {
                        result.addAll(recursiveList(fs, child.getPath()));
                    } else {
                        result.add(child.getPath());
                    }
                }
            }
        } catch (Exception e) {
            log.info("A problem occurred recursively deleting path: " + path + " " + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Check to see if the current context is within an executing task.
     * 
     * @param conf
     * @return 
     */
    private boolean isTask(Configuration conf) {
        return conf.get("mapred.task.id") != null;
    }
    
    /**
     * Handles the various options for when failure should occur.
     * 
     * @param conf
     * @return 
     */
    private boolean shouldFail(Configuration conf) {
        boolean isTask = isTask(conf);
        
        return (!isTask && failOnError) || (isTask && taskFailOnError);
    }
    
    /**
     * Disables listing.  Once this is set, it cannot be re-enabled through
     * the configuration object.
     */
    private void disable() {
        log.warn("Disabling s3mper listing consistency.");
        disabled = true;
    }
}
