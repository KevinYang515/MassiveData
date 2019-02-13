import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FrequentItemsetsMapper extends Mapper<Object, Text, Text, Text>{
	
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		Text word1 = new Text("1");
		
		context.write(values, word1);
		
	}
}