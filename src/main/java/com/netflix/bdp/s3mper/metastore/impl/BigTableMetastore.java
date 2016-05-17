package com.netflix.bdp.s3mper.metastore.impl;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.collect.ImmutableList;
import com.netflix.bdp.s3mper.metastore.FileInfo;
import com.netflix.bdp.s3mper.metastore.FileSystemMetastore;
import com.netflix.bdp.s3mper.metastore.Metastore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * @author liljencrantz@spotify.com
 */
public class BigTableMetastore implements FileSystemMetastore {

    private static final Logger log = Logger.getLogger(BigTableMetastore.class);

    private static final String projectId = "steel-ridge-91615";
    private static final String zone = "europe-west1-c";
    private static final String clusterId = "s3mper";
    private static final String TABLE_NAME = "metadata";
    private static final String COLUMN_FAMILY_NAME = "md";

    private Connection connection;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void initalize(URI uri, Configuration conf) throws Exception {
        try {
            connection = BigtableConfiguration.connect(projectId, zone, clusterId);

            Admin admin = connection.getAdmin();

            // Create a table with a single column family
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
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
            String rowKey = parent.toString();
            Scan scan = new Scan(Bytes.toBytes(rowKey));

            ResultScanner scanner = getTable().getScanner(scan);
            for (Result row : scanner) {
                NavigableMap<byte[], byte[]> data = row.getFamilyMap(Bytes.toBytes(COLUMN_FAMILY_NAME));
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
        Path rowkey = path.getParent();
        Put put = new Put(Bytes.toBytes(rowkey.toUri().toString()));
        URI relative = path.toUri().relativize(rowkey.toUri());

        String jsonBlob = directory ? "{\"isDirectory\": true}" : "{\"isDirectory\": false}" ;

        put.addColumn(
                Bytes.toBytes(COLUMN_FAMILY_NAME),
                Bytes.toBytes(relative.toString()),
                Bytes.toBytes(jsonBlob));

        getTable().put(put);
    }

    private Table getTable() throws IOException {
        return connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    @Override
    public void delete(Path path) throws Exception {

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
        fs.initalize(null, null);
        fs.add(new Path("//hadoopha/tmp/axeltest"), false);
        System.out.println(
                fs.list(ImmutableList.of(new Path("//hadoopha/tmp"))));
    }
}
