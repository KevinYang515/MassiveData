import java.io.IOException;

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


public class WordCountReducer extends Reducer<DoubleWritable, IntWritable, IntWritable, DoubleWritable>{
	
	public void reduce(DoubleWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
		
		for(IntWritable val :value){	
			key.set(key.get()*-1);
			
			context.write(val, key);
		}
	}
}
