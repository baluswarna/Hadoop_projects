package volatility;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapReduce2 {

	public static class Mapper2 extends TableMapper<Text, FloatWritable>  {
		public static final byte[] CF = "adjclose".getBytes();
	    public static final byte[] NAME = "stockname".getBytes();
	    public static final byte[] PRICE = "rateofReturn".getBytes();
		 
	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,InterruptedException {
	   	    try{
	   	    	
	   	 Text stockname = new Text();
	   	 FloatWritable adjclose = new FloatWritable();
	    	String adjcloseprice = new String(value.getValue(CF,PRICE));
	    	String stock = new String(value.getValue(CF,NAME));
	    	String[] stocksymbol = stock.split(" ");
	    	stockname.set(stocksymbol[0]);
	    	adjclose.set(Float.parseFloat(adjcloseprice));
	    	
	    	//System.out.println(stockname.toString()+":"+adjclose.toString());
	    	context.write(stockname,adjclose);
	   	    }catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   	}
		 
	}	
	public static class Reducer2 extends TableReducer<Text, FloatWritable, ImmutableBytesWritable>  {
		DecimalFormat df = new DecimalFormat("#.##########");
		
	 	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
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
			if(volatility!=0 && !(volatility.isNaN())){
				Put p2 = new Put(Bytes.toBytes(key.toString()));
	    		p2.add(Bytes.toBytes("volatility"), Bytes.toBytes("stockname"), Bytes.toBytes(key.toString()));
	    		p2.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatilityvalue"), Bytes.toBytes(df.format(volatility)));
	            
	    		context.write(null, p2);		
	 	}
	 	}
	}
	
}
