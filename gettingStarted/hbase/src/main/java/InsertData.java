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
import java.util.*;

public class InsertData {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("EMP"));

        Put put1 = new Put(Bytes.toBytes("2"));
        //create put for rowId 2
        put1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("337168"));
        put1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("05181971"));
        put1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Ryan"));
        put1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Gosling"));
        put1.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("55000"));
        put1.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("755"));

        Put put2 = new Put(Bytes.toBytes("3"));
        put2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("337169"));
        put2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("06211971"));
        put2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Kevin"));
        put2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Jones"));
        put2.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("75000"));
        put2.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("777"));

        Put put3 = new Put(Bytes.toBytes("4"));
        put3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("337188"));
        put3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("11211971"));
        put3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Justin"));
        put3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Timberlake"));
        put3.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("88000"));
        put3.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("755"));

        Put put4 = new Put(Bytes.toBytes("5"));
        put4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("337144"));
        put4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("06011971"));
        put4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Lance"));
        put4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Park"));
        put4.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("95000"));
        put4.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("755"));

        Put put5 = new Put(Bytes.toBytes("6"));
        put5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("337155"));
        put5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("11211991"));
        put5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Justin"));
        put5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Trudeau"));
        put5.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("88000"));
        put5.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("777"));

        Put put6 = new Put(Bytes.toBytes("7"));
        put6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("347144"));
        put6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("05011971"));
        put6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Tim"));
        put6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Park"));
        put6.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("95000"));
        put6.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("784"));

        Put put7 = new Put(Bytes.toBytes("8"));
        put7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("357155"));
        put7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("01211991"));
        put7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Justin"));
        put7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Trueman"));
        put7.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("88000"));
        put7.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("784"));

        Put put8 = new Put(Bytes.toBytes("9"));
        put8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"), Bytes.toBytes("358155"));
        put8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"), Bytes.toBytes("01191991"));
        put8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"), Bytes.toBytes("Kaplan"));
        put8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"), Bytes.toBytes("Trueman"));
        put8.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"), Bytes.toBytes("86000"));
        put8.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"), Bytes.toBytes("785"));


        List<Put> puts = new ArrayList<Put>();
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        puts.add(put5);
        puts.add(put6);
        puts.add(put7);
        puts.add(put8);

        table.put(puts);
    }
}
