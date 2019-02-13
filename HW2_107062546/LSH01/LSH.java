import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LSH {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        Job job = Job.getInstance(config, "Locality Sensitive Hashing");
        job.setJarByClass(LSH.class);
        
        job.setMapperClass(LSHMap.class);
        job.setReducerClass(LSHReduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
//      job.setPartitionerClass(LSHPartition.class);
//      job.setCombinerClass(WordCountReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
      
        FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/Total.txt"));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
