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

import com.netflix.bdp.s3mper.common.PathUtil;
import com.netflix.bdp.s3mper.metastore.impl.MetastoreJanitor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

/**
 * Deletes entries from the Metastore using the timeseries index.
 * 
 * @author dweeks
 */
public class MetastoreTimeseriesDeleteCommand extends Command {
    
    @Option(name="-ru",usage="Max read units to consume")
    private int readUnits = 100;
    
    @Option(name="-wu",usage="Max write units to consume")
    private int writeUnits = 200;
    
    @Option(name="-s",usage="Numer of scan threads")
    private int scanThreads = 1;
    
    @Option(name="-d",usage="Number of delete threads")
    private int deleteThreads = 10;
    
    @Option(name="-u",usage="Time unit (days, hours, minutes)")
    private String unitType = "Days";
    
    @Option(name="-n",usage="Number of specified units")
    private int unitCount = 1;
    
    @Argument
    private List<String> args = new ArrayList<String>();

    @Override
    public void execute(Configuration conf, String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("s3mper meta delete_ts [options]");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            System.err.println(" Example: s3mper meta delete_ts "+parser.printExample(OptionHandlerFilter.ALL));

            return;
        }
        
        MetastoreJanitor janitor = new MetastoreJanitor();
        janitor.initalize(PathUtil.S3N, conf);
        janitor.setScanLimit(readUnits);
        janitor.setDeleteLimit(writeUnits);
        janitor.setScanThreads(scanThreads);
        janitor.setDeleteThreads(deleteThreads);
        
        janitor.deleteTimeseries(TimeUnit.valueOf(unitType.toUpperCase()), unitCount);
    }
    
    
    
}
