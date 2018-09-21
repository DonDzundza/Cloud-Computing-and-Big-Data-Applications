package stubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestProcessLogs {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	  @Before
	  public void setUp() {

	    /*
	     * Set up the mapper test harness.
	     */
	    LogFileMapper mapper = new LogFileMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver.setMapper(mapper);

	    /*
	     * Set up the reducer test harness.
	     */
	    SumReducer reducer = new SumReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
	    reduceDriver.setReducer(reducer);

	    /*
	     * Set up the mapper/reducer test harness.
	     */
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	  }

	  /*
	   * Test the mapper.
	   */
	  @Test
	  public void testMapper() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     * TODO: implement
	     */
		  String testText = "10.223.157.186 - - [15/Jul/2009:21:24:17 -0700] \"GET /assets/img/media.jpg HTTP/1.1\" 200 110997/r/n10.223.157.186 - - [15/Jul/2009:21:24:18 -0700] \"GET /assets/img/pdf-icon.gif HTTP/1.1\" 200 228/r/n10.216.113.172 - - [16/Jul/2009:02:51:28 -0700] \"GET / HTTP/1.1\" 200 7616/r/n10.216.113.172 - - [16/Jul/2009:02:51:29 -0700] \"GET /assets/js/lowpro.js HTTP/1 .1\" 200 10469/r/n10.216.113.172 - - [16/Jul/2009:02:51:29 -0700] \"GET /assets/css/reset.css HTTP/1.1\" 200 1014";
		  mapDriver.withInput(new LongWritable(1), new Text(testText));
		  mapDriver.withOutput(new Text("10.223.157.186"), new IntWritable(1));
		  mapDriver.withOutput(new Text("10.223.157.186"), new IntWritable(1));
		  mapDriver.withOutput(new Text("10.216.113.172"), new IntWritable(1));
		  mapDriver.withOutput(new Text("10.216.113.172"), new IntWritable(1));
		  mapDriver.withOutput(new Text("10.216.113.172"), new IntWritable(1));
		  mapDriver.runTest();


	  }

	  /*
	   * Test the reducer.
	   */
	  @Test
	  public void testReducer() {

	    /*
	     * For this test, the reducer's input will be "cat 1 1".
	     * The expected output is "cat 2".
	     * TODO: implement
	     */

		  List<IntWritable> values172 = new ArrayList<IntWritable>();
		  values172.add(new IntWritable(1));
		  values172.add(new IntWritable(1));
		  values172.add(new IntWritable(1));
		  reduceDriver.withInput(new Text("10.216.113.172"), values172);
		  reduceDriver.withOutput(new Text("10.216.113.172"), new IntWritable(3));
	      reduceDriver.runTest();
	  

	  }


	  /*
	   * Test the mapper and reducer working together.
	   */
	  @Test
	  public void testMapReduce() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     * The expected output (from the reducer) is "cat 2", "dog 1". 
	     * TODO: implement
	     */
		  String testText = "10.223.157.186 - - [15/Jul/2009:21:24:17 -0700] \"GET /assets/img/media.jpg HTTP/1.1\" 200 110997/r/n10.223.157.186 - - [15/Jul/2009:21:24:18 -0700] \"GET /assets/img/pdf-icon.gif HTTP/1.1\" 200 228/r/n10.216.113.172 - - [16/Jul/2009:02:51:28 -0700] \"GET / HTTP/1.1\" 200 7616/r/n10.216.113.172 - - [16/Jul/2009:02:51:29 -0700] \"GET /assets/js/lowpro.js HTTP/1 .1\" 200 10469/r/n10.216.113.172 - - [16/Jul/2009:02:51:29 -0700] \"GET /assets/css/reset.css HTTP/1.1\" 200 1014";
		  mapReduceDriver.withInput(new LongWritable(1), new Text(testText));
		  mapReduceDriver.withOutput(new Text("10.216.113.172"), new IntWritable(3));
		  mapReduceDriver.withOutput(new Text("10.223.157.186"), new IntWritable(2));

		  mapReduceDriver.runTest();
	 

	  }
}
