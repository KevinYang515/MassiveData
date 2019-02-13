import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountReducer
        extends Reducer<Text,Text,Text,Text> {

    private Text value = new Text(); 
    
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	String str = new String();
  
    	str = "1.0 " + str;
    	
        for (Text val : values) {
            str = str + " " +val.toString();
        }
        value.set(str);
        key.set(key);
        context.write(key, value);
    }
}