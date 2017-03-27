/**
 * https://github.com/Abhijeet87/HBase/
 * Created by Abhijeet on 3/25/17.
 * Objective: To retrieve a list of values from Hbase Table EMP using GET
 * equivlent of GET command on Hbase shell
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;

public class GetListdata {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("EMP"));


        List<Get> gets = new ArrayList<Get>();

        Get get1 = new Get(Bytes.toBytes("2"));

        get1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get1.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get1.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get1.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));

        Get get2 = new Get(Bytes.toBytes("3"));

        get2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get2.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get2.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get2.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));

        Get get3 = new Get(Bytes.toBytes("4"));

        get3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get3.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get3.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get3.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));

        Get get4 = new Get(Bytes.toBytes("5"));

        get4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get4.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get4.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get4.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));

        Get get5 = new Get(Bytes.toBytes("6"));

        get5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get5.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get5.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get5.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));

        Get get6 = new Get(Bytes.toBytes("7"));

        get6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get6.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get6.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get6.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));

        Get get7 = new Get(Bytes.toBytes("8"));

        get7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get7.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get7.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get7.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));


        Get get8 = new Get(Bytes.toBytes("9"));

        get8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("EMP_NO"));
        get8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("DATEOFBIRTH"));
        get8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("FNAME"));
        get8.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("LNAME"));
        get8.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("salary"));
        get8.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("DEPT_NO"));


        gets.add(get1);
        gets.add(get2);
        gets.add(get3);
        gets.add(get4);
        gets.add(get5);
        gets.add(get6);
        gets.add(get7);
        //gets.add(get8);

        Result[] results = table.get(gets);
        for (Result result : results) {
            printAllValues(result);
        }
    }

    private static void printAllValues(Result result){
        NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> resultMap =
                result.getMap();
        for(byte[] columnFamily:resultMap.keySet()){
            String cf = Bytes.toString(columnFamily);
            NavigableMap<byte[],NavigableMap<Long,byte[]>> columnMap = resultMap.get(columnFamily);
            for(byte[] column:columnMap.keySet()){
                String col = Bytes.toString(column);
                NavigableMap<Long,byte[]> timestampMap = columnMap.get(column);
                for(Long timestamp:timestampMap.keySet()) {
                    String ts = timestamp.toString();
                    String value = Bytes.toString(timestampMap.get(timestamp) );
                    System.out.println("Column Family: " + cf
                            + " Column: "+col+" Value: "+value );
                }
            }
        }
    }

}
