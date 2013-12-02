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

import com.google.common.collect.Maps;
import com.netflix.bdp.s3mper.alert.impl.AlertJanitor;
import com.netflix.bdp.s3mper.common.PathUtil;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author dweeks
 */
public class S3mper extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            CMD cmd = CMD.valueOf(args[0].toUpperCase());
            
            switch (cmd) {
                case FS:
                case FILESYSTEM:
                    processFileSystem(popArg(args));
                    break;
                case META:
                case METASTORE:
                    processMetastore(popArg(args));
                    break;
                case SQS:
                    processSQS(popArg(args));
                    break;
                default:
                    usage();                
            }
        } catch (Exception exception) {
            usage();
        }
        
        return 0;
    }
    
    enum CMD { FS, FILESYSTEM, META, METASTORE, SQS };
    enum SQS_CMD { LOG, PURGE };
    enum META_CMD { LIST, RESOLVE, DELETE_PATH, DELETE_TS };
    
    public static void main(String[] args) throws Exception {    
        ToolRunner.run(new Configuration(), new S3mper(), args);
    }
    
    private void processFileSystem(String [] args) throws Exception {
        Map<String, Command> commands = Maps.newHashMap();
        commands.put("verify", new FileSystemVerifyCommand());
        
        Command command = commands.get(args[0]);
        command.execute(getConf(), popArg(args));
    }
    
    private void processMetastore(String [] args) throws Exception {
        Map<META_CMD, Command> commands = Maps.newHashMap();
        commands.put(META_CMD.LIST, new MetastoreListCommand());
        commands.put(META_CMD.RESOLVE, new MetastoreResolveCommand());
        commands.put(META_CMD.DELETE_PATH, new MetastorePathDeleteCommand());
        commands.put(META_CMD.DELETE_TS, new MetastoreTimeseriesDeleteCommand());
        try {
            
            META_CMD cmd = META_CMD.valueOf(args[0].toUpperCase());
            
            Command command = commands.get(cmd);
            command.execute(getConf(), popArg(args));
        } catch (Exception e) {
            System.out.println("Command options are: list, resolve, delete_path, delete_ts\n");
            
            e.printStackTrace();
        }
    }
    
    private void processSQS(String [] args) throws Exception {
        GnuParser parser = new GnuParser();
        
        Options options = new Options();
        
        CommandLine cmdline = parser.parse(options, args);
        
        SQS_CMD cmd = SQS_CMD.valueOf(cmdline.getArgs()[0].toUpperCase());
        
        
        AlertJanitor janitor = new AlertJanitor();
        janitor.initalize(PathUtil.S3N, new Configuration());
        String queue = janitor.resolveQueueUrl(cmdline.getArgs()[1]);
        
        switch(cmd) {
            case LOG:
                janitor.writeLogs(queue, new Path(cmdline.getArgs()[2]));
                break;
            case PURGE:
                janitor.clearAll(queue);
                break;
            default: 
                usage();
        }
    }
    
    private String [] popArg(String [] args) {
        if(args.length == 1) {
            return new String[0];
        }
        
        return Arrays.copyOfRange(args, 1, args.length);
    }
    
    private void usage() {
        System.out.println("Command options are: filesystem, metastore, sqs.\n");
    }
    
}
