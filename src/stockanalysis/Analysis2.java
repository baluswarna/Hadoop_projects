package stockanalysis;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;



public class Analysis2 {

	public static class Map2 extends Mapper<LongWritable,Text,Text,FloatWritable>{
		
		Text stock = new Text();
		FloatWritable adjclose = new FloatWritable();
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString();
			String[] rec = line.split("\t");
			String[] stocksymbol = rec[0].split("-");
			stock.set(stocksymbol[0]);
			adjclose.set(Float.parseFloat(rec[rec.length-1]));
			context.write(stock, adjclose);
		}
	}
	public static class Reduce2 extends Reducer<Text,FloatWritable,Text,Text>{
		Text stock = new Text();
		Text var = new Text();
		//FloatWritable volatility = new FloatWritable();
		DecimalFormat df = new DecimalFormat("#.##########");
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException,InterruptedException{
			Double sum= new Double("0.0f");
			Double volsum=  new Double("0.0f");
			Double xdash,volatility;
			ArrayList<Float> xvalues = new ArrayList<Float>();
			
			for(FloatWritable value: values){
				xvalues.add(value.get());
			}
			
			for(Float x: xvalues){
				 sum += x;
			}
			
			xdash = sum/xvalues.size();
			
			for(Float x: xvalues){
				volsum += Math.pow(x-xdash, 2);
			}
			volatility = Math.sqrt(volsum/(xvalues.size()-1));
			System.out.println(key+ Double.toString(volatility));
			stock.set(key);
			if(volatility!=0 && !(volatility.isNaN())){
				var.set(df.format(volatility));
			context.write(stock,var );
			}
			}
	}

}

