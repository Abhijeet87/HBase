/**
 * Created by cloudera on 3/25/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class UpdateData {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("EMP"));

//HTable table = new HTable(conf, "notifications");

        Put put1 = new Put(Bytes.toBytes("2"));
        //Update salary for rowId 2
        put1.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("65000"));

        table.put(put1);
    }
}