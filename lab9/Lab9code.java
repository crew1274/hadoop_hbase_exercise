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

public class Lab9 {

	static Configuration cfg = null;
	static {
		Configuration HBASE_CONFIG = new Configuration();
		//code here
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
			System.out.println("Table [" + tableName + "] is created successfully!");
		}
	}
	
	public static void put(String tableName, String row, String columnFamily, String qualifier, String data) throws Exception{
		HTable table = new HTable(cfg, tableName);
		//code here
		System.out.println("put '"+ row +"', '"+ columnFamily + ":" + qualifier + "','" + data +"'");
	}
	
	public static void get(String tableName, String row, String columnFamily, String qualifier) throws IOException{
		HTable table = new HTable(cfg, tableName);
		//code here
		System.out.println("get '"+ row +"', '"+ columnFamily +":"+qualifier+"': " +val);
	}
	
	public static void scan(String tableName) throws IOException{
		HTable table = new HTable(cfg, tableName);
		//code here
		System.out.println("========scan start========");
		for (Result r : rs){
			//code here
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
			Lab9.create(tableName, columnFamily);
			//code here
			Lab9.scan(tableName);
		} 
		catch (Exception e){
			e.printStackTrace();
		} 

	}

}
