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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.netflix.bdp.s3mper.common.PathUtil;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

/**
 *
 * @author dweeks
 */
public class FileSystemVerifyCommand extends Command {
    private static final Logger log = Logger.getLogger(FileSystemVerifyCommand.class.getName());
    
    @Option(name="-t",usage="Number of threads")
    private int threads = 10;
    
    @Option(name="-f",usage="Number of threads")
    private String file = null;
    
    private AmazonS3Client s3;
    
    @Override
    public void execute(Configuration conf, String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        
        String keyId = conf.get("fs.s3n.awsAccessKeyId");
        String keySecret = conf.get("fs.s3n.awsSecretAccessKey");
        
        s3 = new AmazonS3Client(new BasicAWSCredentials(keyId, keySecret));
        
        try {
            parser.parseArgument(args);
            
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future> futures = new ArrayList<Future>();
            
            BufferedReader fin = new BufferedReader(new FileReader(file));
            
            try {
                for(String line = fin.readLine(); line != null; line = fin.readLine()) {
                    futures.add(executor.submit(new FileCheckTask(new Path(line.trim()))));
                }
            } finally {
                fin.close();
            }
            
            for(Future f : futures) {
                f.get();
            }
            
            executor.shutdown();
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("s3mper fs verify [options]");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            System.err.println(" Example: s3mper fs verify "+parser.printExample(OptionHandlerFilter.ALL));
        }
    }
    
    private class FileCheckTask implements Callable<Boolean> {
        private Path path;

        public FileCheckTask(Path path) {
            this.path = path;
        }
        
        @Override
        public Boolean call() throws Exception {
            boolean exists = false;
                 
            FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
            
            try {
                exists = fs.exists(path);
            } catch (Exception e) { }

            System.out.println((exists?"[EXISTS ] ":"[MISSING] ") + path.toUri());
            
            return exists;
        }
    }
    
}
