package stockanalysis;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import stockanalysis.Analysis1.Map1;
import stockanalysis.Analysis1.Reduce1;
import stockanalysis.Analysis2.Map2;
import stockanalysis.Analysis2.Reduce2;
import stockanalysis.Analysis3.Map3;
import stockanalysis.Analysis3.Reduce3;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			// Create a new Job
		     Job job = Job.getInstance();
		     job.setJarByClass(Analysis1.class);
			
			job.setMapperClass(Map1.class);
			//job.setCombinerClass(Reduce1.class);
			job.setReducerClass(Reduce1.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);
			
			/*job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);*/
			
			Job job1 = Job.getInstance();
			job1.setJarByClass(Analysis2.class);
			job1.setMapperClass(Map2.class);
			job1.setReducerClass(Reduce2.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(FloatWritable.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(FloatWritable.class);
			
			job1.setNumReduceTasks(1);
			
			Job job2 = Job.getInstance();
			job2.setJarByClass(Analysis3.class);
			job2.setMapperClass(Map3.class);
			job2.setReducerClass(Reduce3.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path("Inter_"+args[1]));
			FileInputFormat.addInputPath(job1, new Path("Inter_"+args[1]));
			FileOutputFormat.setOutputPath(job1, new Path("OutputInter_"+args[1]));
			FileInputFormat.addInputPath(job2, new Path("OutputInter_"+args[1]));
			FileOutputFormat.setOutputPath(job2, new Path("Output_"+args[1]));
			
			//job.setJarByClass(Analysis2.class);
			job.waitForCompletion(true);
			job1.waitForCompletion(true);
			job2.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
