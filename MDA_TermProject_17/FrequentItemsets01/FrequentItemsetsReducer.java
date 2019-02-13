import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequentItemsetsReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for (Text val : values) {
			count = count + Integer.parseInt(val.toString());
		}
		
		Text word2 = new Text(count + "");
		
		if(count >= 5) {
			context.write(key, word2);
		}
	}

}
