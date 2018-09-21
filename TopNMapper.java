package stubs;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  Mapper's input are read from SequenceFile and records are: (K, V)
 *    where 
 *          K is a Text
 *          V is an Integer
 * 
 * @author Mahmoud Parsian
 *
 */
public class TopNMapper extends
   Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void setup(Context context) throws IOException,
	         InterruptedException {
	       int N = context.getConfiguration().getInt("N", 10); // default is top 10
	   }
   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
	   String text = value.toString();
	   String[] lines = text.split("/r/n");
	   for (String line:lines){
		   String[] info = line.split(",");
		   String movieID = info[0];
		   String movieRate = info[2];

		   int rate = Integer.valueOf(movieRate.toCharArray()[0]);
		   context.write(new Text(movieID), new IntWritable(rate));
	   }
      
   }
   

  

}