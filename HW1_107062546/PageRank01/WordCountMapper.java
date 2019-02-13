import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCountMapper extends Mapper<Object, Text, Text, Text>{

	private Text word1 = new Text();
	private Text word2 = new Text();
	private Text word3 = new Text();
	private Text word4 = new Text();
	
	
	@Override
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
	    while (st.hasMoreTokens()) {
	        word1.set(st.nextToken());
	        word2.set(st.nextToken());
	        
	        word3 = word1;
	        word4 = word2;
	        
	        context.write(word3, word4);
	    }
	}
}
