package stockanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Analysis1 {

	public static class Map1 extends Mapper<LongWritable,Text,Text,Text> {
		
		Text month = new Text();
		Text adjclose= new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			String record = value.toString();
			String[] details = record.split(",");
			if(details[0].contains("-")){
			String[] datesplit = details[0].split("-");
			String fn= ((Path)((FileSplit)context.getInputSplit()).getPath()).getName();
			month.set(fn.substring(0, fn.indexOf("."))+"-"+datesplit[1]+datesplit[0]);
			adjclose.set(datesplit[1]+datesplit[2]+"/"+details[details.length-1]);
			context.write(month, adjclose);
			}
		}
	}
	
	
	public static class Reduce1 extends Reducer <Text,Text,Text,Text>{
              
		Text month = new Text();
		Text adjclose= new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			ArrayList<String> prices = new ArrayList<String>();
				for(Text value:values){
			prices.add(value.toString());
		}
			
			Collections.sort(prices);
			String start = prices.get(0);
	    	Float begin = Float.parseFloat(start.substring(start.indexOf("/")+1,start.length()));
	    	String end = prices.get(prices.size()-1);
	    	Float monthend = Float.parseFloat(end.substring(end.indexOf("/")+1,end.length()));
	    	Float roR= (monthend-begin)/begin;
			month.set(key);
			adjclose.set(Float.toString(roR));
			context.write(month, adjclose);
				}
     }		
	
}
