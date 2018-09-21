package stubs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.xerial.snappy.Snappy;

public class SequenceFileWriter extends Configured implements Tool {

	private static List<String> readFile(String file) throws IOException{
		  	List<String> set = new ArrayList<String>();
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
	        buf.close();
		  	for (int i = 0; i<unfiltered.length; i++){
		  		if (Character.isDigit(unfiltered[i].charAt(0))){
		  			set.add(unfiltered[i]);
		  		}
		  	}
		  	
		  	return set;
		}
	
      public static void main(String[] args) throws Exception {
		    int exitCode = ToolRunner.run(new Configuration(), (Tool) new SequenceFileWriter(), args);
		    System.exit(exitCode);
		  }
	  public int run(String[] args) throws Exception {

    String uri = "/home/training/testlog/output";
    String dataPath = "/home/training/testlog/test_access_log";
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    Text key = new Text();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    
    try {

      writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
      List<String> lineList = readFile(dataPath);    
      for (int i = 0; i < lineList.size(); i++) {
    	String IPstring = lineList.get(i).split(" -")[0];
        key.set(new Text(IPstring));
        value.set(new Text(lineList.get(i)));
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
	return 0;

  }
}