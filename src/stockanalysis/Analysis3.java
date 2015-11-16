package stockanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Analysis3 {
	
	public static class Map3 extends Mapper<LongWritable,Text,Text,Text>{
		
		Text dummy = new Text("Key");
		Text value = new Text();
		
		public void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException{
			String[] record = value.toString().split(" ");
			value.set(record[record.length-1]+"/"+record[0]);
			context.write(dummy, value);
		}
		
	}

	
	public static class Reduce3 extends Reducer<Text,Text,Text,Text>{
		ArrayList<String> al = new ArrayList<String>();
		Text key = new Text();
		Text value = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			String[] split ;
			
			for(Text value: values){
				al.add(value.toString());
			}
			
			Collections.sort(al);
			context.write(new Text("Top 10 stocks with minimum volatility "), new Text(" "));
			
			for(int i=0; i<10;i++){
				
				split = al.get(i).split("/");
				key.set(split[1]);
				value.set(split[0]);
				context.write(key,value);
			
			}
			
					
			/*for(int i = 1;i<10;i++){
				split= al.get(al.size()-i).split("/");
				context.write(new Text(split[1]),new Text(split[0]));
			}*/
		}
	}
	
	
}

