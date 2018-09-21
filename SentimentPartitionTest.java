package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner mpart;

	@Test
	public void testSentimentPartition() {
         SentimentPartitioner spart = new SentimentPartitioner();
         spart.setConf(new Configuration());
         int result;
         
         result = spart.getPartition(new Text("love"), new IntWritable(1), new Integer(3));
         assertEquals(0,result);
         result = spart.getPartition(new Text("deadly"), new IntWritable(1), new Integer(3));
         assertEquals(1,result);
         result = spart.getPartition(new Text("zodiac"), new IntWritable(1), new Integer(3));
         assertEquals(2,result);
		
	}

}
