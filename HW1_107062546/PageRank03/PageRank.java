import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
		
        Job job = Job.getInstance(config, "PageRank03");
        job.setJarByClass(PageRank.class);
        
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
           
        job.setMapOutputKeyClass(DoubleWritable.class);
//        job.setCombinerClass(WordCountReducer.class);
        
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputValueClass(DoubleWritable.class);
        
//        job.setSortComparatorClass(sortComparator.class);
        
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
//        Path p = fs.getFileStatus(new Path(".")).getPath(); 
//        URI(p.toString()).getPath(); 
//        
//        System.out.println(path.toUri().getRawPath());
//        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
