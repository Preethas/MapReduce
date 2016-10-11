
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MovieRunner {
   public static void main(String[] args) throws IOException{
	   if (args[0] == null || args[1] == null || args[2] == null){
			System.out.println("Usage MovieRunner <inputFile> " +
					"<outputDir> <integer choice 1: User Rating 2: Movie Rating> <order 1:MIN 2:MAX>");
	   }
	   JobConf conf = new JobConf();
	   conf.setJobName("MovieAnalytics");
	   conf.set("choice", args[2]);
	   conf.set("order", args[3]);
	   conf.setOutputKeyClass(Text.class);
	   conf.setOutputValueClass(Text.class);
	   
	   
	   conf.setMapperClass(MovieMapper.class);
	   conf.setReducerClass(MovieReducer.class);
	   conf.setCombinerClass(MovieReducer.class);
	   
	   conf.setInputFormat(TextInputFormat.class);
	   conf.setOutputFormat(TextOutputFormat.class);
	   
	   FileInputFormat.setInputPaths(conf, new Path(args[0]));
	   FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	   
	   
	   try{
	      JobClient.runJob(conf);
	   }catch(Exception e){
		   e.printStackTrace();
	   }

   }
}
