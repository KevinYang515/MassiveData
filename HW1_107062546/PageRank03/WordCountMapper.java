import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapper extends Mapper<Object, Text, DoubleWritable, IntWritable>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
//		Text node = new Text();
//		double rank = 0.0;
//		ArrayList<String> tonode = new ArrayList<String>();
//		
//		if(itr.hasMoreTokens()) {
//			node.set(itr.nextToken());
//			Double.parseDouble(itr.nextToken());
//		}
//		
//		while(itr.hasMoreTokens()) {
//			tonode.add(itr.nextToken());
//		}
		
		Text node = new Text();
		node.set(key.toString());
		
		if(itr.hasMoreTokens())
		{
			IntWritable intwritable = new IntWritable(Integer.parseInt(itr.nextToken().toString()));
			DoubleWritable doublewritable = new DoubleWritable(Double.parseDouble(itr.nextToken().toString())*-1) ;
			
			context.write(doublewritable, intwritable);
			
		}
		
	}
}
