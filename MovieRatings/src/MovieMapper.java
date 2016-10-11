import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MovieMapper extends MapReduceBase 
   implements Mapper<LongWritable,Text,Text,Text> {

	Text mapkey = new Text();
	int CHOICE= 1; // User Rating
	
	public void configure(JobConf jobconf){
		String choice  = jobconf.get("choice");
		CHOICE = Integer.parseInt(choice);
		System.out.println("Choice " + CHOICE);
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output,Reporter reporter)
			throws IOException {
		
		String line = value.toString();
		String[] tokens = line.split("\\s+");
		if (CHOICE == 1){
		  mapkey.set(tokens[0]);
		}else{
		  mapkey.set(tokens[1]);
		}
		output.collect(mapkey,new Text(tokens[3]));
	}

}
