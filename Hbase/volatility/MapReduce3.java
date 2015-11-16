package volatility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class MapReduce3 {

	public static class Mapper3 extends TableMapper<Text, Text>  {
		public static final byte[] CF = "volatility".getBytes();
	    public static final byte[] NAME = "stockname".getBytes();
	    public static final byte[] VOLATILITY = "volatilityvalue".getBytes();
		 
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,InterruptedException {
	   	    try{
	   	    	Text dummy = new Text("dummy");
	   	    	Text record = new Text();
	   	    	
	   	    	String volatility = new String(value.getValue(CF,VOLATILITY));
		    	String stock = new String(value.getValue(CF,NAME));
		    	record.set(volatility+"/"+stock);
		    	context.write(dummy, record);
		    	
	   	    }catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   	}
	}
	
	public static class Reducer3 extends Reducer<Text, Text, Text,Text>  {
		
		ArrayList<String> al = new ArrayList<String>();
		Text key = new Text();
		Text value = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
String[] split ;
			
			for(Text value: values){
				//context.write(key,value);
				al.add(value.toString());
			}
			
			Collections.sort(al);
			context.write(new Text("Top 10 stocks with lowest (min) volatility "), new Text(" "));
			System.out.println("Top 10 stocks with lowest (min) volatility ");
			/*key.set(al.get(0));
			value.set(al.get(al.size()-1));
			context.write(key,value);*/
			for(int i=0; i<10;i++){
				
				split = al.get(i).split("/");
				key.set(split[1]);
				value.set(split[0]);
				System.out.println(key.toString()+" "+value.toString());
				context.write(key,value);
	            		
			}
			
			context.write(new Text("Top 10 stocks with highest (max) volatility "), new Text(" "));	
			System.out.println("Top 10 stocks with highest (max) volatility ");
			for(int i = 1;i<10;i++){
				split= al.get(al.size()-i).split("/");
				context.write(new Text(split[1]),new Text(split[0]));
				System.out.println(split[1]+" "+split[0]);
			}
		}
	}
}
