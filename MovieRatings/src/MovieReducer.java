import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MovieReducer extends MapReduceBase implements 
Reducer<Text,Text,Text,Text>{
	
    int ORDER = 1; // 1 : Minimum 2 : Maximum
	
    public void configure(JobConf jobconf){
		String order  = jobconf.get("order");
		ORDER = Integer.parseInt(order);
		System.out.println("Order " + ORDER);
	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		Integer MIN = Integer.MAX_VALUE;
		Integer MAX = Integer.MIN_VALUE;
		
		List<Integer> itemList = new ArrayList<Integer>();
		
		while(values.hasNext()){
			Text val = values.next();
			itemList.add(Integer.parseInt(val.toString()));
		}
		
		if (itemList.size() == 1) {
			MIN = itemList.get(0);
			MAX = itemList.get(0);
		} else {
    	  for (int i=1;i < itemList.size();i++){
    		int f = itemList.get(i);
    		int s = itemList.get(i-1);
    		int diff = Math.abs(f-s);
    		if (diff < MIN) MIN = diff;
    		if (diff > MAX) MAX = diff;
    	  }
		}
		if (ORDER == 1){
		  output.collect(key, new Text(MIN + ""));
		} else {
		  output.collect(key, new Text(MAX + ""));
		}
	}
}
