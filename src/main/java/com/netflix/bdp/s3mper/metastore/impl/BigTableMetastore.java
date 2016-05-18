package com.netflix.bdp.s3mper.metastore.impl;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.collect.ImmutableList;
import com.netflix.bdp.s3mper.common.RetryTask;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import com.netflix.bdp.s3mper.metastore.Metastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author liljencrantz@spotify.com
 */
public class BigTableMetastore implements FileSystemMetastore {

    private static final Logger log = Logger.getLogger(BigTableMetastore.class);

    private static final String projectId = "steel-ridge-91615";
    private static final String zone = "europe-west1-c";
    private static final String clusterId = "s3mper";
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("md");

    private Connection connection;
    private static TableName tableName;
    private final ObjectMapper mapper = new ObjectMapper();

    private int retryCount = Integer.getInteger("s3mper.metastore.retry", 3);
    private int timeout = Integer.getInteger("s3mper.metastore.timeout", 5000);

    @Override
    public void initalize(URI uri, Configuration conf) throws Exception {
        try {
            tableName = TableName.valueOf(conf.get("s3mper.metastore.name", "metadata"));
            retryCount = conf.getInt("s3mper.metastore.retry", retryCount);
            timeout = conf.getInt("s3mper.metastore.timeout", timeout);

            connection = BigtableConfiguration.connect(projectId, zone, clusterId);

            Admin admin = connection.getAdmin();

            // Create a table with a single column family
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
            try {
                admin.createTable(descriptor);
            }
            catch(IOException e) {
                // Ignore existing table
                // Fixme: Ignores a bunch of other crap as well. :-/
            }
        }
        catch (Exception e) {
            log.error("Error while initializing metastore", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<FileInfo> list(List<Path> parents) throws Exception {
        ImmutableList.Builder<FileInfo> result = ImmutableList.builder();

        // Now scan across all rows.
        for (Path parent: parents) {
            ResultScanner scanner = new RetryTask<ResultScanner>(
                    new ScanTask(parent), retryCount, timeout).call();

            for (Result row : scanner) {
                NavigableMap<byte[], byte[]> data = row.getFamilyMap(COLUMN_FAMILY_NAME);
                for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
                    String name = Bytes.toString(entry.getKey());
                    String jsonBlob = Bytes.toString(entry.getValue());
                    Map mmm = mapper.readValue(jsonBlob, HashMap.class);
                    result.add(new FileInfo(new Path(parent, name), false, (Boolean) mmm.get("isDirectory")));
                }
            }
        }
        return result.build();
    }

    @Override
    public void add(Path path, boolean directory) throws Exception {
        new RetryTask(new AddTask(path, directory), retryCount, timeout).call();
    }

    private Table getTable() throws IOException {
        return connection.getTable(tableName);
    }

    @Override
    public void delete(Path path) throws Exception {
        new RetryTask(new DeleteTask(path), retryCount, timeout).call();
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
            log.error("Error while closing metastore", e);
            throw new RuntimeException(e);
        }
        connection = null;
    }

    @Override
    public int getTimeout() {
        return 0;
    }

    @Override
    public void setTimeout(int timeout) {

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystemMetastore fs = Metastore.getFilesystemMetastore(conf);

        fs.initalize(null, conf);

        fs.add(new Path("//hadoopha/tmp/axeltest1"), false);
        fs.add(new Path("//hadoopha/tmp/axeltest2"), false);

        System.out.println("List 1: " +
                fs.list(ImmutableList.of(new Path("//hadoopha/tmp"))));

        fs.delete(new Path("//hadoopha/tmp/axeltest2"));

        System.out.println("List 2: " +
                fs.list(ImmutableList.of(new Path("//hadoopha/tmp"))));
    }

    /**
     * A Callable task for use with RetryTask to add a path to the
     * DynamoDB table.
     */
    private class AddTask implements Callable<Object> {

        private final Path path;
        private final boolean directory;

        public AddTask(Path path, boolean directory) {
            this.path = path;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            Path rowkey = path.getParent();
            Put put = new Put(Bytes.toBytes(rowkey.toUri().toString()));
            String basename = path.getName();
            String jsonBlob = directory ? "{\"isDirectory\": true}" : "{\"isDirectory\": false}";

            put.addColumn(
                    COLUMN_FAMILY_NAME,
                    Bytes.toBytes(basename.toString()),
                    Bytes.toBytes(jsonBlob));

            getTable().put(put);
            return null;
        }

    }

    private class DeleteTask implements Callable<Object> {

        private final Path path;

        public DeleteTask(Path path) {
            this.path = path;
        }

        @Override
        public Object call() throws Exception {
            byte[] rowkey = Bytes.toBytes(path.getParent().toUri().toString());
            byte[] basename = Bytes.toBytes(path.getName());

            Delete delete = new Delete(rowkey);
            delete.addColumns(COLUMN_FAMILY_NAME, basename);

            getTable().delete(delete);

            return null;
        }

    }

    private class ScanTask implements Callable<ResultScanner> {

        private final Path parent;

        public ScanTask(Path parent) {
            this.parent = parent;
        }

        @Override
        public ResultScanner call() throws Exception {
            return getTable().getScanner(new Scan(Bytes.toBytes(parent.toString())));
        }

    }

}
