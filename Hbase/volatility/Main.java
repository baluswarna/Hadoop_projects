package volatility;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



public class Main{


	public static void main(String[] args){

		Configuration conf = HBaseConfiguration.create();
		try {
			
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw1"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("raw1")){
				admin.disableTable("raw1");
				admin.deleteTable("raw1");
			}
			admin.createTable(tableDescriptor);


			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job, new Path("data"));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("raw1", null, job);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			
			
			HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("firststage"));
			tableDescriptor1.addFamily(new HColumnDescriptor("adjclose"));
			if (admin.isTableAvailable("firststage")){
			admin.disableTable("firststage");
			admin.deleteTable("firststage");
			}
			admin.createTable(tableDescriptor1);
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);
			Job job1 = Job.getInstance();
			job1.setJarByClass(MapReduce1.class);
			TableMapReduceUtil.initTableMapperJob(
			 "raw1",        // input table
			 scan,               // Scan instance to control CF and attribute selection
			 MapReduce1.Mapper1.class,     // mapper class
			 Text.class,         // mapper output key
			 Text.class,  // mapper output value
			 job1);
			TableMapReduceUtil.initTableReducerJob(
			 "firststage",        // output table
			 MapReduce1.Reducer1.class,    // reducer class
			 job1);
			job1.setNumReduceTasks(1);
			job1.waitForCompletion(true);
			
			HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("secondstage"));
			tableDescriptor2.addFamily(new HColumnDescriptor("volatility"));
			if (admin.isTableAvailable("secondstage")){
			admin.disableTable("secondstage");
			admin.deleteTable("secondstage");
			}
			admin.createTable(tableDescriptor2);
			Scan scan2 = new Scan();
			scan2.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan2.setCacheBlocks(false);
			Job job2 = Job.getInstance();
			job2.setJarByClass(MapReduce2.class);
			TableMapReduceUtil.initTableMapperJob(
			 "firststage",        // input table
			 scan2,               // Scan instance to control CF and attribute selection
			 MapReduce2.Mapper2.class,     // mapper class
			 Text.class,         // mapper output key
			 FloatWritable.class,  // mapper output value
			 job2);
			TableMapReduceUtil.initTableReducerJob(
			 "secondstage",        // output table
			 MapReduce2.Reducer2.class,    // reducer class
			 job2);
			job2.setNumReduceTasks(1);
			job2.waitForCompletion(true);
			
			HTableDescriptor tableDescriptor3 = new HTableDescriptor(TableName.valueOf("thirdstage"));
			tableDescriptor3.addFamily(new HColumnDescriptor("volatility"));
			if (admin.isTableAvailable("thirdstage")){
			admin.disableTable("thirdstage");
			admin.deleteTable("thirdstage");
			}
			admin.createTable(tableDescriptor3);
			Scan scan3 = new Scan();
			scan3.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan3.setCacheBlocks(false);
			Job job3 = Job.getInstance();
			job3.setJarByClass(MapReduce3.class);
			TableMapReduceUtil.initTableMapperJob(
			 "secondstage",        // input table
			 scan3,               // Scan instance to control CF and attribute selection
			 MapReduce3.Mapper3.class,     // mapper class
			 Text.class,         // mapper output key
			 Text.class,  // mapper output value
			 job3);
			job3.setReducerClass(MapReduce3.Reducer3.class);    // reducer class
			job3.setNumReduceTasks(1);    // at least one, adjust as required
			FileOutputFormat.setOutputPath(job3, new Path("Out_data/out.txt"));
			job3.waitForCompletion(true);
			
			
			
						admin.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}


