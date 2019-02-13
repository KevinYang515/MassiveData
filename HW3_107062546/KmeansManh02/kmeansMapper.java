import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class kmeansMapper extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		String a[] = values.toString().split(" "); 
		
		Text file1 = new Text();
		Text file2 = new Text();
		
		String string = "";
		
		for(int i = 1; i < a.length; i ++) {
			if(i < a.length - 1) {
				string = string + a[i] + " ";
			}
			else {
				string = string + a[i];
			}
		}
		
		file1.set(a[0]);
		file2.set(string);
		
		context.write(file1, file2);
	}
}
