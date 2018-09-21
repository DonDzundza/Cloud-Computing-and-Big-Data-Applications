package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Boolean caseSensitive = true;
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", true);
	}
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

      String line = value.toString();
      
      for(String word : line.split("\\W+")) {
    	  if (!caseSensitive) {
    		  word = word.toLowerCase();
    	  }
    	  int len = word.length();
    	  if(len > 0){
    		  String a = Character.toString(word.charAt(0));
    		  context.write(new Text(a), new IntWritable(len));
    	  }
      }
  }
  
}
