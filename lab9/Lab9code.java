package lab9;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class lab9 {

    static Configuration cfg = null;
    static {
        Configuration HBASE_CONFIG = new Configuration();
        //code here
        HBASE_CONFIG.set("hbase.zookeeper.quorem","localhost");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        
        cfg = HBaseConfiguration.create(HBASE_CONFIG);
    }
    
    public static void create(String tableName, String columnFamily) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)){
            System.out.println("Table [" + tableName + "] is already Exists!");
            System.exit(0);
        }
        else {
            //code here
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("Table [" + tableName + "] is created successfully!");
        }
    }
    
    public static void put(String tableName, String row, String columnFamily, String qualifier, String data) throws Exception{
        HTable table = new HTable(cfg, tableName);
        //code here
        Put p1 = new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),Bytes.toBytes(data));
        table.put(p1);
        System.out.println("put '"+ row +"', '"+ columnFamily + ":" + qualifier + "','" + data +"'");
    }
    
    public static void get(String tableName, String row, String columnFamily, String qualifier) throws IOException{
        HTable table = new HTable(cfg, tableName);
        //code here
        Get g = new Get(Bytes.toBytes(row));
        Result result = table.get(g);
        byte [] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        String val = Bytes.toString(value);
        System.out.println("get '"+ row +"', '"+ columnFamily +":"+qualifier+"': " +val);
    }
    
    public static void scan(String tableName) throws IOException{
        HTable table = new HTable(cfg, tableName);
        //code here
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        System.out.println("========scan start========");
        for (Result r : rs){
            //code here
            get(tableName,Bytes.toString(r.getRow()),"cf","school");
            get(tableName,Bytes.toString(r.getRow()),"cf","dept");
            get(tableName,Bytes.toString(r.getRow()),"cf","id");
        }
        System.out.println("========scan stop========");
    }
    
    public static boolean delete(String tableName) throws IOException{
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)){
            try{
                //code here
                
            }
            catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }
        }
        return true;
    }
    
    public static void main(String[] args) {
        String tableName = "Lab9_Table";
        String columnFamily = "cf";
        try{
            lab9.create(tableName, columnFamily);
            //code here
            lab9.put(tableName, "row1", columnFamily , "school", "NCKU");
            lab9.put(tableName, "row1", columnFamily , "dept", "ES");
            lab9.put(tableName, "row1", columnFamily , "id", "N96044183");
            lab9.put(tableName, "row2", columnFamily , "school", "CCU");
            lab9.put(tableName, "row2", columnFamily , "dept", "CSIE");
            lab9.put(tableName, "row2", columnFamily , "id", "604410064");
            lab9.get(tableName, "row1", columnFamily , "id");
            lab9.scan(tableName);
        } 
        catch (Exception e){
            e.printStackTrace();
        } 

    }

}
