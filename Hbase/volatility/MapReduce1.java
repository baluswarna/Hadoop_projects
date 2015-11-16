package volatility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Text;

public class MapReduce1 {
public static class Mapper1 extends TableMapper<Text, Text>  {
	public static final byte[] STOCK = "stock".getBytes();
    public static final byte[] NAME = "name".getBytes();
    public static final byte[] TIME = "time".getBytes();
    public static final byte[] DD = "dd".getBytes();
    public static final byte[] MM = "mm".getBytes();
    public static final byte[] YR = "yr".getBytes();
    //public static final byte[] PRICECOL = "pricecol".getBytes();
    public static final byte[] PRICE = "price".getBytes();
	 
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,InterruptedException {
   	    try{
   	    	//System.out.println("hi");
   	    
   	 Text stockname = new Text();
   	 Text adjclose = new Text();
    	String adjcloseprice = new String(value.getValue(PRICE,PRICE));
    	String stock = new String(value.getValue(STOCK,NAME));
    	String year = new String(value.getValue(TIME, YR));
    	String month = new String(value.getValue(TIME, MM));
    	String day = new String(value.getValue(TIME, DD));
    	stockname.set(stock+" "+month+year);
    	adjclose.set(day+"/"+adjcloseprice);
    	
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

public static class Reducer1 extends TableReducer<Text, Text, ImmutableBytesWritable>  {
 
	
 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 		 String priceval;
         float monthbegin=0,monthend=0;
         TreeMap<String,String> adj_close = new TreeMap<String,String>();
         for (Text val: values){
             priceval = val.toString();
        //     System.out.println(token);
             adj_close.put(priceval.split("/")[0], priceval.split("/")[1]);
         }
         Map.Entry<String,String> firstelem = adj_close.firstEntry();
         Map.Entry<String,String> lastelem = adj_close.lastEntry();
	    	monthbegin = Float.parseFloat(firstelem.getValue());
            monthend = Float.parseFloat(lastelem.getValue());
	    	//System.out.println(monthend);
	    	Float rateofreturn= (monthend-monthbegin)/(monthbegin);
	    	
	    	//System.out.println(key.toString()+":"+rateofreturn.toString());
	    	Put p1 = new Put(Bytes.toBytes(key.toString()));
    		p1.add(Bytes.toBytes("adjclose"), Bytes.toBytes("stockname"), Bytes.toBytes(key.toString()));
    		p1.add(Bytes.toBytes("adjclose"), Bytes.toBytes("rateofReturn"), Bytes.toBytes(Float.toString(rateofreturn)));
            
    		context.write(null, p1);
   	}
}
}
