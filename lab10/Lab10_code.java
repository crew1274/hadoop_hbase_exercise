import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Lab10 {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable i = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//code here...
			String str[] = value.toString().trim().split(" ");
			for (String s: str) {
				context.write(new Text(s), i);
			}
		}
	}
	public static class Reduce extends TableReducer<Text, IntWritable, NullWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			//code here...
			int sum = 0;
			for (IntWritable i: values) {
				sum += i.get();
			}

			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("context"), Bytes.toBytes("Count"), Bytes.toBytes(String.valueOf(sum)));
			context.write(NullWritable.get(), put);
		}
	}
	public static void createHBaseTable(String tableName) throws IOException{
		//code here...
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor col = new HColumnDescriptor("context");
		htd.addFamily(col);
		HBaseConfiguration config = new HBaseConfiguration();
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.tableExists(tableName)){
			System.out.println("Table [" + tableName + "] already exists, trying to recreate table...");
			//code here...
			admin.disableTable(tableName);
			admin.disableTable(tableName);
		}
		System.out.println("Creating the new table ["+ tableName + "]...");
		//code here...
		admin.createTable(htd);
	}
	
	public static void main(String args[]) throws Exception{
		//code here...
		String tableName = "Lab10";
		Configuration conf = new Configuration();
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		//code here...
		createHBaseTable(tableName);
		String input = args[0];
		Job job = new Job(conf,"WordCount table with " + input);
		job.setJarByClass(Lab10.class);
		job.setNumReduceTasks(3);
		//code here...
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		System.exit(job.waitForCompletion(true)?0:1);
	}


}
