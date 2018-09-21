package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  double sum = 0.0;
	  double cnt = 0.0;
	  for(IntWritable length : values){
		  cnt++;
		  sum += length.get();
	  }
	  Double ave = sum/cnt;
	  
	  context.write(key, new DoubleWritable(ave));

  }
}