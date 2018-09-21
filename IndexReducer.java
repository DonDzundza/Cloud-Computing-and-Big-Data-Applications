package stubs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

	    String finalValue = "";
        for (Text value : values) {

			finalValue += "," + value;
		}
		String finalVALUE = "";
        for (int i = 1; i<finalValue.length(); i++){
        	finalVALUE += Character.toString(finalValue.charAt(i));
        }
		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new Text(finalVALUE));
    /*
     * TODO implement
     */
    
  }
}