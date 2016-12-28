package lab11;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CsvToHBase {
    static Configuration cfg = null;
    static {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "localhost");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        cfg = HBaseConfiguration.create(HBASE_CONFIG);
    }
    
    
    public static void putIntoHBase(String tableName, String rowId ,String[] data) throws IOException{
        HTable table = new HTable(cfg, tableName);
        Put p = new Put(Bytes.toBytes(rowId));
        //code here
        System.out.println("put '"+ rowId +"', cf1:School, '" + data[0] +"'");
        System.out.println("put '"+ rowId +"', cf1:Dept, '" + data[1] +"'");
        System.out.println("put '"+ rowId +"', cf2:Id, '" + data[2] +"'");
    }
    
    
    public static void createHBaseTable(String tableName) throws IOException{
        HTableDescriptor htd = new HTableDescriptor(tableName);
        //code here
        HBaseConfiguration config = new HBaseConfiguration();
        HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(tableName)){
            System.out.println("Table [" + tableName + "] already exists, trying to recreate table...");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        System.out.println("Creating the new table ["+ tableName + "]...");
        admin.createTable(htd);
    }
    
    public static void main(String[] args) throws Exception{
        String tableName = "csvtohbase";
        createHBaseTable(tableName);
        //code here
    }
}

