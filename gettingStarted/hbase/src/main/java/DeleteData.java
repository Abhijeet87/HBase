/**
 * https://github.com/Abhijeet87/HBase/
 * Created by Abhijeet on 25-03-17
 * Objective: To delete contents of a Row ID (employee details)*
 * equivalent of DELETE command on Hbase shell
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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