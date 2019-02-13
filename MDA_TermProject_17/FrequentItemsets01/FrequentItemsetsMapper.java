import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequentItemsetsMapper extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		String singer[] = values.toString().split(",");
		
		Text word2 = new Text();
		word2.set("1");
		
		for(int i = 0; i < singer.length; i ++) {
			Text word1 = new Text();
			word1.set(singer[i]);
			
			context.write(word1, word2);
		}

	}
}
