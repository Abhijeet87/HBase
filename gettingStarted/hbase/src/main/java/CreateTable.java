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

/**
 * Created by cloudera on 3/25/17.
 */
public class CreateTable {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        //  Boiler plate for creating connections

        Admin admin = connection.getAdmin();
        /* We use HbaseAdmin object to
        1.create tables
        2. delete tables
        3. check if a table exists etc */

        HTableDescriptor tableName = new HTableDescriptor("EMP");

        /*HTableDescriptor is used to specify
        1. The table name
        2. Column families
        3. Some other properties related to
        performance tuning
        */

        tableName.addFamily(new HColumnDescriptor("attributes"));
        tableName.addFamily(new HColumnDescriptor("metrics"));

        if (!admin.tableExists(tableName.getTableName())) {
            System.out.print("Creating table. ");
            admin.createTable(tableName);
            System.out.println(" Done.");

            connection.close();
        }
    }
}










