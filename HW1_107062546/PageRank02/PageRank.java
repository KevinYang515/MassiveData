import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PageRank {

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        
        Path inPath = new Path(otherArgs[0]);
        Path outPath =  null;
        for(int i = 0; i < 20; i++) {
        	outPath = new Path(otherArgs[1]+"-"+i);
        	
        	Job job = new Job(conf, "PageRank02");
            job.setJarByClass(PageRank.class);
    		
            job.setMapperClass(WordCountMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(WordCountReducer.class);
    		
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
    		
            FileInputFormat.addInputPath(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);
            job.waitForCompletion(true);
            inPath = outPath;
        }
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}
