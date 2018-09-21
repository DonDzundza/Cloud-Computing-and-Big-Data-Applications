package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	String fileName = new String();
	public void setup(Context context){
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
	    fileName = fileSplit.getPath().getName();
	}
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
	    String text = value.toString();
	    text = text.toLowerCase();
	    String[] lines = text.split("\n");
	    for (String singleLine : lines) {
	    	String index = singleLine.split("\t")[0];
	    	String Sentence = singleLine;
	    	for (String word : Sentence.split("\\W+")){
	    		if(word != null && !word.matches(".*\\d+.*")){
	    		String fileIndex = fileName + "@" + index;
	    		context.write(new Text(word), new Text(fileIndex));}
	    	}
	    }

 }
	
    
  }
