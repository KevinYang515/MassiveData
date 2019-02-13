import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class kmeansReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text word1 = new Text();
		Text word2 = new Text();
		
		int count = 0;
		ArrayList<ArrayList<Double>> array = new ArrayList<ArrayList<Double>>();
		
		for (Text val : values) {
			String num[] = val.toString().split(" ");
			
			ArrayList<Double> array1 = new ArrayList<Double>();
			
			for(int i = 0 ; i < num.length; i ++) {
				array1.add(Double.parseDouble(num[i]));
			}
			array.add(array1);
			count++;
        }
		
		ArrayList<Double> ans = new ArrayList<Double>();
		
		for(int i = 0; i < array.get(0).size(); i ++) {
			ans.add(0.0);
		}
		
		for(int i = 0; i < count; i ++) {
			for(int j = 0; j < array.get(i).size(); j ++) {
				ans.set(j, ans.get(j) + array.get(i).get(j));
			}
		}
		
		for(int i = 0; i < ans.size(); i ++) {
			ans.set(i, ans.get(i)/count);
		}
		
		word1.set(key);
		
		String string = "";
		
		for(int i = 0; i < ans.size(); i ++) {
			if(i < ans.size() - 1) {
				string = string + ans.get(i) + " ";
			}else {
				string = string + ans.get(i);
			}
			
		}
		
		word2.set(string);
		
		context.write(word1, word2);
		
	}
}
