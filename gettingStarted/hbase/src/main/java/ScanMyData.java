/**
 * https://github.com/Abhijeet87/HBase/
 * Created by Abhijeet on 25-03-17
 * Objective: To scan contents of a Hbase Table (EMP)*
 * equivalent to Hbase shell command 'scan'
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;

public class ScanMyData {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("EMP"));

        Scan fullScan = new Scan();
        ResultScanner fullScanResult = table.getScanner(fullScan);
        for (Result res : fullScanResult) {
            printAllValues(res);
        }
        fullScanResult.close();

     /*
     // Some other formats of scan
     //     column based scan
        Scan colScan = new Scan();
        colScan.addFamily(Bytes.toBytes("metrics"));
        ResultScanner colScanResult = table.getScanner(colScan);
        for (Result res:colScanResult){
            printAllValues(res);
        }
        colScanResult.close();

        // Range based scan
        Scan rangeScan = new Scan();
        rangeScan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("type"))
                .setStartRow(Bytes.toBytes("2"))
                .setStopRow(Bytes.toBytes("2"));
        ResultScanner rangeScanResult = table.getScanner(rangeScan);
        for (Result res:rangeScanResult){
            printAllValues(res);
        }
        rangeScanResult.close();
    }
    */

    }

    // method to convert map format data into string format and display as output
    private static void printAllValues(Result result) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap =
                result.getMap();
        for (byte[] columnFamily : resultMap.keySet()) {
            String cf = Bytes.toString(columnFamily);
            NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = resultMap.get(columnFamily);
            for (byte[] column : columnMap.keySet()) {
                String col = Bytes.toString(column);
                NavigableMap<Long, byte[]> timestampMap = columnMap.get(column);
                for (Long timestamp : timestampMap.keySet()) {
                    String ts = timestamp.toString();
                    String value = Bytes.toString(timestampMap.get(timestamp));
                    System.out.println("Column Family: " + cf
                            + " Column: " + col + " Value: " + value);
                }
            }
        }
    }
}