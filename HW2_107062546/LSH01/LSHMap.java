import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LSHMap extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		
		Text file1 = new Text();
		Text file2 = new Text();
 		String a[] = values.toString().split(","); 
		int filenum = Integer.parseInt(a[0]);
		
		filenum = (filenum - 1) / 2;
		filenum = filenum + 1;
		
		String string = "";
		
		for(int i = 1; i <= 50; i++) {
			
			if(i <= 49) {
				
				string = string + a[i] + ",";
			}else {
				
				string = string + a[i];
			}
		}
		
		file1.set(filenum+"");
		file2.set(values);
		
		context.write(file1, file2);
		
//		StringTokenizer itr = new StringTokenizer(values.toString());		
		
//		Text file = new Text();
//		ArrayList<String> tonodelist = new ArrayList<String>();
//		
//		String line;
//		
//		if(itr.hasMoreTokens()) {
//			line = itr.nextToken().toString();
//			rank = Double.parseDouble(itr.nextToken());
//		}
//
//		
//		while(itr.hasMoreTokens()){
//			tonodelist.add(itr.nextToken());
//		}
		
	}
}
