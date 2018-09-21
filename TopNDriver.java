package stubs;


import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * TopNDriver: assumes that all K's are unique for all given (K,V) values.
 * Uniqueness of keys can be achieved by using AggregateByKeyDriver job.
 *
 * @author Mahmoud Parsian
 *
 */
public class TopNDriver  extends Configured implements Tool {


	public static void main(String[] args) throws Exception {

	    /*
	     * The expected command-line arguments are the paths containing
	     * input and output data. Terminate the job if the number of
	     * command-line arguments is not exactly 2.
	     */
		  
			int exitCode = ToolRunner.run(new Configuration(), new TopNDriver(), args);
			System.exit(exitCode);
		}
	  public int run(String[] args) throws Exception{
	    if (args.length != 3) {
	      System.out.printf(
	          "Usage: N <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    /*
	     * Instantiate a Job object for your job's configuration.  
	     */
	      int N = Integer.parseInt(args[0]); // top N
	      Configuration conf = getConf();
	      conf.setInt("N",N);
	    Job job = new Job(conf);
	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(TopNDriver.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("Top N List");

	    /*
	     * Specify the paths to the input and output data based on the
	     * command-line arguments.
	     */
	    FileInputFormat.setInputPaths(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

	    /*
	     * Specify the mapper and reducer classes.
	     */
	    job.setMapperClass(TopNMapper.class);
	    job.setReducerClass(TopNReducer.class);
	    /*
	     * For the word count application, the input file and output 
	     * files are in text format - the default format.
	     * 
	     * In text format files, each record is a line delineated by a 
	     * by a line terminator.
	     * 
	     * When you use other input formats, you must call the 
	     * SetInputFormatClass method. When you use other 
	     * output formats, you must call the setOutputFormatClass method.
	     */
	      
	    /*
	     * For the word count application, the mapper's output keys and
	     * values have the same data types as the reducer's output keys 
	     * and values: Text and IntWritable.
	     * 
	     * When they are not the same data types, you must call the 
	     * setMapOutputKeyClass and setMapOutputValueClass 
	     * methods.
	     */

	    /*
	     * Specify the job's output key and value classes.
	     */
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	  }


}