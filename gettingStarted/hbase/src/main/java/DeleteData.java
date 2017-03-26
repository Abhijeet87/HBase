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

public class DeleteData{
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf("EMP"));

        Delete delete = new Delete(Bytes.toBytes("9"));
        /*
        delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        delete.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        delete.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        delete.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));
        */

        table.delete(delete);
        table.close();
    }
}