package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();

  /**
   * Set up the positive and negative hash set in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration) {
     /*
     * Add the positive and negative words to the respective sets using the files 
     * positive-words.txt and negative-words.txt.
     */
    /*
     * TODO implement 
     */ 
	  try {
		  
		  
	//	positive = readFile("/home/training/workspace/wordcount/src/stubs/positive-words.txt");
	//  The above line is used to run test locally!!!!!
    //  Don't forget to change this while you are testing my SentimentPartitionTest.java
		
		positive = readFile("positive-words.txt");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  try {
		//negative = readFile("/home/training/workspace/wordcount/src/stubs/negative-words.txt");
		//  The above line is used to run test locally!!!!!!!
		//  Don't forget to change this while you are testing my SentimentPartitionTest.java
		
		negative = readFile("negative-words.txt");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
  }
  private Set<String> readFile(String file) throws IOException{
	  	Set<String> set = new HashSet<String>();
	    InputStream is = new FileInputStream(file);
	    BufferedReader buf = new BufferedReader(new InputStreamReader(is));
	  	String line = buf.readLine();
	  	StringBuilder sb = new StringBuilder();
	  	while(line != null){
	  		sb.append(line).append("/n");
	  		line = buf.readLine();
	  	}
        String f = sb.toString();
        String[] unfiltered = f.split("/n");
	  	for (int i = 0; i<unfiltered.length; i++){
	  		if (unfiltered[i].charAt(0) != ';'){
	  			set.add(unfiltered[i]);
	  		}
	  	}
	  	
	  	return set;
	}
  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
	 if(positive.contains(key.toString())){
		 return 0;
	 } else if(negative.contains(key.toString())){
		 return 1;
	 } else return 2;

  }
}
