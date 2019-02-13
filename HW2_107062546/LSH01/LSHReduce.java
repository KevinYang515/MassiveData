import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LSHReduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
		
		Text word1 = new Text();
		Text word2 = new Text();
		
		Map<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> list0= new ArrayList<Integer>();
		ArrayList<Integer> list1= new ArrayList<Integer>();
		ArrayList<Integer> list2= new ArrayList<Integer>();
		ArrayList<Integer> list3= new ArrayList<Integer>();
		ArrayList<Integer> list4= new ArrayList<Integer>();
		ArrayList<Integer> list5= new ArrayList<Integer>();
		ArrayList<Integer> list6= new ArrayList<Integer>();
		ArrayList<Integer> list7= new ArrayList<Integer>();
		ArrayList<Integer> list8= new ArrayList<Integer>();
		ArrayList<Integer> list9= new ArrayList<Integer>();
		
		
		int a[][] = new int[2][50];
		int count = 0;
		
		for (Text val : values) {
			String num[] = val.toString().split(",");
			
			for(int i = 0 ; i < 50; i++) {
				a[count][i] = Integer.parseInt(num[i]);
			}
			
			count++;
        }
		
		
		for(int i = 0; i < 50 ; i++) {
			
			int hash = (a[0][i] + a[1][i]) % 10;
			
			if(hash == 0 && a[0][i] != 0 && a[1][i] != 0){
				list0.add(i);
			}else if(hash == 1) {
				list1.add(i);
			}else if(hash == 2) {
				list2.add(i);
			}else if(hash == 3) {
				list3.add(i);
			}else if(hash == 4) {
				list4.add(i);
			}else if(hash == 5) {
				list5.add(i);
			}else if(hash == 6) {
				list6.add(i);
			}else if(hash == 7) {
				list7.add(i);
			}else if(hash == 8) {
				list8.add(i);
			}else if(hash == 9) {
				list9.add(i);
			}
		}
		
		input(map, list0, 0);
		input(map, list1, 1);
		input(map, list2, 2);
		input(map, list3, 3);
		input(map, list4, 4);
		input(map, list5, 5);
		input(map, list6, 6);
		input(map, list7, 7);
		input(map, list8, 8);
		input(map, list9, 9);
		
		String string = "";
		
		for (Integer num : map.keySet() ) {
			ArrayList<Integer> arr = map.get(num);
			String str = "";
			
			for(int i = 0; i < arr.size(); i++) {
				str = str + arr.get(i);
				if(i < arr.size() - 1) {
					str = str + ",";
				}
			}
			
		    string = string + num +":" + str + ";";
		}
		
		word1 = key;
		word2.set(string);
		context.write(word1, word2);
		
	}
	
	public void input(Map<Integer, ArrayList<Integer>> map, ArrayList<Integer>list, int num) {
		
		if(list.size()>1) {
			map.put(num, list);
		}
	}
}
