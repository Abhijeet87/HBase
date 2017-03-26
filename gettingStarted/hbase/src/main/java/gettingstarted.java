/**
 * Created by cloudera on 3/20/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class gettingstarted {

  public static void main(String[] args) throws IOException {

      Configuration conf = HBaseConfiguration.create();
      //  conf.set("hbase.thrift.info.bindAddress","0.0.0.0");
    Connection connection = ConnectionFactory.createConnection(conf);
    //Admin admin = connection.getAdmin();
    Table table = connection.getTable(TableName.valueOf("demo"));
    //Table table = connection.getTable("demo");

    Scan scan1 = new Scan();
    ResultScanner scanner1 = table.getScanner(scan1);

    for (Result res:scanner1) {
        System.out.println(res);
    }
    scanner1.close();
    connection.close();
    }
}
