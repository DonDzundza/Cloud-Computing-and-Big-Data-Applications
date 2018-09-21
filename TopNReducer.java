package stubs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.httpclient.methods.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer's input are local top N from all mappers.
 *  We have a single reducer, which creates the final top N.
 * 
 * @author Mahmoud Parsian
 *
 */
public class TopNReducer  extends
Reducer<Text, IntWritable, IntWritable, Text> {

	private int N = 10; // default
	private SortedMap<Integer, Integer> top = new TreeMap<Integer, Integer>();

	@Override
	protected void setup(Context context) 
			throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10); // default is top 10
	}
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		int rateSum = 0;
		for (IntWritable value : values) {

			rateSum += value.get();
		}
		String movieID = key.toString();
		int id = Integer.valueOf(movieID);
		top.put(rateSum,id);

		while(top.size()>N){
			top.remove(top.firstKey());
		}
		// emit final top N

	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		List<Integer> keys = new ArrayList<Integer>(top.values());
		HashMap<Integer, String> idtoti = new HashMap<Integer, String>();
		BufferedReader br = new BufferedReader(new FileReader("src/movie_titles.txt"));
		String f = "";
		List<String> lines = new ArrayList<>();
		while((f = br.readLine())!=null){
			lines.add(f);
		}
		for(String line: lines){
				String[] info = line.split(",");
				int ID = Integer.valueOf(info[0]);
				String title = info[2];
				idtoti.put(ID,title);
		}


		for(int i=keys.size()-1; i>=0; i--){
			int ID = keys.get(i);
			String title = idtoti.get(ID);

			if(title != null){
				context.write(new IntWritable(ID), new Text(title));}
		}
	}




}