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

import com.netflix.bdp.s3mper.alert.impl.CloudWatchAlertDispatcher;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.impl.DynamoDBMetastore;
import com.netflix.bdp.s3mper.metastore.impl.MetastoreJanitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 * @author dweeks
 */
public class ConsistentListingAspectTest {

    private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";

    private static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";

    private static final Logger log = Logger.getLogger(ConsistentListingAspectTest.class.getName());
    
    private static DynamoDBMetastore meta;
    private static CloudWatchAlertDispatcher alert;
    private static MetastoreJanitor janitor;
    
    private static Configuration conf;
    private static FileSystem markerFs;
    private static FileSystem deleteFs;
    private static Path testPath;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        for (final String envVar : asList(AWS_ACCESS_KEY_ID,
                                          AWS_SECRET_ACCESS_KEY)) {
            if (isNullOrEmpty(System.getenv(envVar))) {
                fail("Required environment variable " + envVar + " is not defined");
            }
        }

        conf = new Configuration();

        conf.set("fs.s3n.awsAccessKeyId", System.getenv(AWS_ACCESS_KEY_ID));
        conf.set("fs.s3n.awsSecretAccessKey", System.getenv(AWS_SECRET_ACCESS_KEY));

        conf.setBoolean("s3mper.disable", false);
        conf.setBoolean("s3mper.failOnError", true);
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", true);
        conf.setBoolean("s3mper.reporting.disabled", true);
        conf.setLong("s3mper.listing.recheck.count", 10);
        conf.setLong("s3mper.listing.recheck.period", 1000);
        conf.setFloat("s3mper.listing.threshold", 1);
        conf.set("s3mper.metastore.name", "ConsistentListingMetastoreTest");
        
        testPath = new Path(System.getProperty("fs.test.path", "s3n://netflix-s3mper-test/test"));
        
        markerFs = FileSystem.get(testPath.toUri(), conf);
        
        Configuration deleteConf = new Configuration(conf);
        deleteConf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
        deleteFs = FileSystem.get(testPath.toUri(), deleteConf);
        
        meta = new DynamoDBMetastore();
        meta.initalize(testPath.toUri(), conf);
        
        alert = new CloudWatchAlertDispatcher();
        alert.init(testPath.toUri(), conf);
        
        Configuration janitorConf = new Configuration(conf);
        janitorConf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
        
        janitor = new MetastoreJanitor();
        janitor.initalize(testPath.toUri(), janitorConf);
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        janitor.clearPath(testPath);

        markerFs.close();
        deleteFs.close();
        meta.close();
        if (janitor != null) {
            janitor.clearPath(testPath);
        }

        if (markerFs != null) {
            markerFs.close();
        }

        if (deleteFs != null) {
            deleteFs.close();
        }

        if (meta != null) {
            meta.close();
        }
    }
    
    @Before
    public void setUp() throws Exception {
        System.out.println("==========================   Setting Up =========================== ");
        
        conf.setBoolean("s3mper.disable", false);
        conf.setBoolean("s3mper.failOnError", true);
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", true);
        conf.setBoolean("s3mper.reporting.disabled", true);
        conf.setLong("s3mper.listing.recheck.count", 10);
        conf.setLong("s3mper.listing.recheck.period", 1000);
        conf.setFloat("s3mper.listing.threshold", 1);
        conf.set("s3mper.metastore.name", "ConsistentListingMetastoreTest");
                
        janitor.clearPath(testPath);
        deleteFs.delete(testPath, true);
    }
    
    @After
    public void tearDown() throws Exception {
        System.out.println("==========================  Tearing Down  =========================");
        conf.setBoolean("s3mper.metastore.deleteMarker.enabled", false);
        janitor.clearPath(testPath);
        deleteFs.delete(testPath, true);
        conf.setFloat("s3mper.listing.threshold", 1);
    }

    @Test
    public void testFileCreateMethods() throws Throwable {
        System.out.println("testFileCreateMethods");
        Path file = new Path(testPath + "/create-methods.test");
        
        //create(Path)
        OutputStream fout = deleteFs.create(file);
        assertNotNull(fout);
        fout.close();
        List<FileInfo> files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(Path, Progressable)");
        fout = deleteFs.create(file, new Progressable(){
            @Override
            public void progress() {
            }
        });
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(Path, boolean)");
        fout = deleteFs.create(file, true);
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(Path, short)");
        fout = deleteFs.create(file, (short) 1);
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(Path, boolean, int)");
        fout = deleteFs.create(file, true, 4096);
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(FileSystem, Path, FsPermission)");
        fout = deleteFs.create(deleteFs, file, FsPermission.getDefault());
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(FileSystem, short, Progressable)");
        fout = deleteFs.create(file, (short)1, new Progressable(){
            @Override
            public void progress() {
            }
        });
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(FileSystem, boolean, int, Progressable)");
        fout = deleteFs.create(file, true, 4096, new Progressable(){
            @Override
            public void progress() {
            }
        });
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(FileSystem, boolean, int, short, long)");
        fout = deleteFs.create(file, true, 4096, (short)1, 100000000);
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
        
        System.out.println("create(FileSystem, boolean, int, short, long, Progressable)");
        fout = deleteFs.create(file, true, 4096, (short)1, 100000000,new Progressable(){
            @Override
            public void progress() {
            }
        });
        assertNotNull(fout);
        fout.close();
        files = meta.list(Collections.singletonList(file.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(file.getParent(), true);
        janitor.clearPath(testPath);
    }
    
    
    
    @Test
    public void testUpdateMetastore() throws Throwable {
        System.out.println("updateMetastore");
        Path arg1Path = new Path(testPath + "/update.test");
        OutputStream fout = deleteFs.create(arg1Path);
        assertNotNull(fout);
        fout.close();
        List<FileInfo> files = meta.list(Collections.singletonList(arg1Path.getParent()));
        assertEquals(1, files.size());
        deleteFs.delete(arg1Path, true);
        janitor.clearPath(testPath);
    }

    @Test
    public void testWritePerformace() throws Throwable {
        System.out.println("testWritePerformace");
        int fileCount = Integer.getInteger("test.file.count", 10);
        int threadCount = Integer.getInteger("test.thread.count", 5);
        
        List<FileCreator> writeThreads = new ArrayList<FileCreator>();
        
        for (int i = 0; i < threadCount; i++) {
            FileCreator f = new FileCreator(deleteFs, fileCount, i);
            f.start();
            
            writeThreads.add(f);
        }
        
        long start = System.currentTimeMillis();
        for(FileCreator t : writeThreads) {
            t.join();
        }
        long stop = System.currentTimeMillis();
        
        for(FileCreator t : writeThreads) {
            log.info(t.getName() + " failures: " + t.failures);
        }
        
        log.info("Total time for writes: " + (stop - start));
        
        List<FileInfo> files = meta.list(Collections.singletonList(testPath));
        
        assertEquals(fileCount * threadCount, files.size());
        janitor.clearPath(testPath);
    }
    
    @Test
    public void testAlerts() throws Exception {
        System.out.println("testAlerts");
        Path alertFile = new Path(testPath+"/alert.test");
        
        conf.setLong("s3mper.listing.recheck.count", 0);
        
        meta.add(alertFile, false);
        
        int count = Integer.getInteger("test.alert.count", 5);
        int sleep = Integer.getInteger("test.alert.sleep", 1000);
        
        int alerts = 0;
        
        for(int i=0; i<count; i++) {
            try {
                deleteFs.listStatus(testPath);
            } catch (Exception e) {
                log.info(format("[%d of %d alerts]: %s",i,count,e.getMessage()));
                alerts++;
            }
            
            Thread.sleep(sleep);
        }
        
        assertEquals("Listing failures didn't match", count, alerts);
        
        meta.delete(alertFile);
    }
    
    @Test
    public void testRecursiveDelete() throws Exception {
        System.out.println("testDeleteDirectory");
        Path p = testPath;
        
        System.out.println("Creating dirs/files");
        for (int level=0; level<5; level++) {
            p = new Path(p, ""+level);
            deleteFs.mkdirs(p);
            deleteFs.create(new Path(p,"file.txt")).close();
        }
        
        p = testPath;
        
        for (int level=0; level<5; level++) {
            p = new Path(p, ""+level);
            assertTrue("Incorrect Entry Count: " + p, meta.list(Collections.singletonList(p)).size() >= 1);                        
        }
        
        System.out.println("Sleeping for 5s");
        Thread.sleep(5000);
        
        System.out.println("Calling delete . . .");
        deleteFs.delete(new Path(testPath+"/0"), true);
        
        System.out.println("Sleeping for 5s");
        Thread.sleep(5000);
        
        p = testPath;
        
        for (int level=0; level<5; level++) {
            p = new Path(p, ""+level);
            
            boolean empty = false;
            
            List<FileInfo> listing = meta.list(Collections.singletonList(p));
            
            if(listing.isEmpty()) {
                empty = true;
            } else {
                empty = true;
                
                for(FileInfo f : listing) {
                    if(!f.isDeleted()) {
                        empty = false;
                        break;
                    }
                }
            }
                
            assertTrue("Entry found for path: " + p, empty);                        
        }
        
        p = testPath;
        for (int level=0; level<5; level++) {
            p = new Path(p, ""+level);
            janitor.clearPath(p);
        }
    }
    
    @Test
    public void testTimeout() throws Exception {
        System.out.println("testTimeout");
        
        int currentTimeout = meta.getTimeout();
        meta.setTimeout(5);
        
        int count = Integer.getInteger("test.timeout.count", 4);
        int sleep = Integer.getInteger("test.timeout.sleep", 1000);
        
        int timeouts = 0;
        
        for (int i = 0; i < count; i++) {
            try {
                meta.list(Collections.singletonList(testPath));
            } catch (Exception e) {
                if( !(e instanceof TimeoutException)) {
                    fail("Caught non-timeout exception");
                }
                
                timeouts++;
                log.info("Caught exception: " + e.getClass());
            }
            
            Thread.sleep(sleep);
        }
        
        meta.setTimeout(currentTimeout);
        
        assertEquals("Timeouts test failed", count, timeouts);
    }
    
    @Test
    public void testTimeoutAlerts() throws Exception {
        System.out.println("testTimeoutAlerts");
        
        for (int i = 0; i < 10; i++) {
            alert.timeout("testAlert", Collections.singletonList(testPath));
        }
    }

    @Test
    public void testRecheckBackoff() throws Exception {
        System.out.println("testRecheckBackoff");
        
        Path path = new Path(testPath.toUri() + "/backoff.test");
        
        meta.add(path, false);
        
        long start = System.currentTimeMillis();
        
        try {
            deleteFs.listStatus(testPath);
        } catch (Exception e) {}
                
        long stop = System.currentTimeMillis();
        long time = stop-start;
        
        meta.delete(path);
        
        System.out.println("Time taken (ms): " + time);
        assertTrue("Recheck Backoff Failed", time > TimeUnit.SECONDS.toMillis(10));
    }
    
    @Test
    public void testDeleteMarker() throws Exception {
        System.out.println("testDeleteMarker");
        
        Path path = new Path(testPath.toUri() + "/deleteMarker.test");
        
        OutputStream fout = deleteFs.create(path);
        fout.close();
        
        List<FileInfo> listing = meta.list(Collections.singletonList(testPath));
        
        assertEquals("Metastore listing size was incorrected", 1, listing.size());
        
        deleteFs.delete(path, true);
        
        listing = meta.list(Collections.singletonList(testPath));
        
        assertEquals("Metastore listing size after delete was incorrected", 1, listing.size());
        assertTrue("Delete marker was not present", listing.get(0).isDeleted());
        
        meta.delete(path);
    }
    
    @Test 
    public void testDeleteMarkerListing() throws Exception {
        Path p1 = new Path(testPath.toUri() + "/deleteMarkerListing-1.test");
        Path p2 = new Path(testPath.toUri() + "/deleteMarkerListing-2.test");
        Path p3 = new Path(testPath.toUri() + "/deleteMarkerListing-3.test");
        
        deleteFs.create(p1).close();
        deleteFs.create(p2).close();
        deleteFs.create(p3).close();
        
        deleteFs.delete(p1, false);
        deleteFs.delete(p3, false);
        
        assertEquals("Wrong number of fs listed files", 1, deleteFs.listStatus(testPath).length);
        assertEquals("Wrong number of metastore listed files", 3, meta.list(Collections.singletonList(testPath)).size());
    }
    
    @Test 
    public void testThreshold() throws Exception {
        Path p1 = new Path(testPath.toUri() + "/deleteMarkerListing-1.test");
        Path p2 = new Path(testPath.toUri() + "/deleteMarkerListing-2.test");
        Path p3 = new Path(testPath.toUri() + "/deleteMarkerListing-3.test");
        
        deleteFs.create(p1).close();
        deleteFs.create(p2).close();
        meta.add(p3, false);
        
        conf.setFloat("s3mper.listing.threshold", 0.5f);
        
        System.out.println("Watining for s3 . . .");
        Thread.sleep(10000);
        
        try {
            FileStatus [] files = deleteFs.listStatus(testPath);
            
            assertEquals("Didn't list the correct number of files", 2, files.length);
        } catch (Exception e) {
            fail("Threshold not met, but should have been");
        }
        
    }
    
    @Test
    public void testRecoveryMessage() throws Exception {
        Path p1 = new Path(testPath.toUri() + "/recovery-1.test");
        final Path p2 = new Path(testPath.toUri() + "/recovery-2.test");
        
        conf.setBoolean("s3mper.reporting.disabled", false);
        
        deleteFs.create(p1).close();
        meta.add(p2, false);
        
        System.out.println("Watining for s3 . . .");
        Thread.sleep(10000);
        
        Thread t = new Thread(new Runnable(){
            @Override
            public void run() {
                try {
                    System.out.println("Creating missing file");
                    Thread.sleep(1500);
                    deleteFs.create(p2).close();
                } catch (Exception e) { }
            }
        }); 
        
        
        t.start();
        deleteFs.listStatus(testPath);
    }
    
    @Test
    public void testTaskFailure() throws Exception {
        Path p1 = new Path(testPath.toUri() + "/task-fail-1.test");
        
        
        
        
        
    }
    
    private static class FileCreator extends Thread {
        int instance;
        int count;
        FileSystem fs;
        
        volatile int failures = 0;
        
        public FileCreator(FileSystem fs, int count, int instance) {
            super("Test FileCreateThread-" + instance);
            this.fs = fs;
            this.count = count;
            this.instance = instance;
        }
        
        @Override
        public void run() {
            try {
                for (int i = 0; i < count; i++) {
                    Path p = new Path(testPath + "/perf-"+instance+"-"+i);
                    
                    log.debug(("Creating file: "+p));
                    try {
                        fs.create(p);
                    } catch (Exception e) {
                        log.error("",e);
                        failures ++;
                    }
                }
            } catch (Exception e) {
                log.error("",e);
            }
        }
        
    }
}