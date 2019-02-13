import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FrequentItemsets {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		
		
		ArrayList<ArrayList<String>> total = new ArrayList<ArrayList<String>>();
		
		Path file1 = new Path(otherArgs[0] + "/output-1.txt");//Location of file in HDFS
		FileSystem fs1 = FileSystem.get(new Configuration());
		BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(file1)));
		
		Path file2 = new Path(otherArgs[0] + "/part-r-00002");//Location of file in HDFS
		FileSystem fs2 = FileSystem.get(new Configuration());
		BufferedReader br2 = new BufferedReader(new InputStreamReader(fs2.open(file2)));
      
		while (br1.ready()){
			String line[] = br1.readLine().split(",");
			ArrayList<String> array = new ArrayList<String>();
			
			for(int i = 0; i < line.length; i ++) {
				array.add(line[i]);
			}
			total.add(array);
		}
		
		
		ArrayList<ArrayList<String>> filter2 = new ArrayList<ArrayList<String>>();
		
		while (br2.ready()){
			String line1[] = br2.readLine().split("\t");
			String line2[] = line1[0].split(",");
			
			ArrayList<String> array1 = new ArrayList<String>();
			
			for(int i = 0; i < line2.length; i ++) {
				array1.add(line2[i]);
			}
			
			filter2.add(array1);
		}
		
		fs1.close();
		fs2.close();
		
		Path output1 = new Path(otherArgs[0] + "/freq3.txt");
		FileSystem fout1 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream1 = fout1.create(output1);
		
		for(int i = 0; i < total.size(); i ++) {
			ArrayList<String> arraylist1 = total.get(i);
			
			for(int j = 0; j < arraylist1.size() - 2; j ++) {
				String string1 = arraylist1.get(j);
				
				for(int k = j + 1; k < arraylist1.size() - 1; k ++) {	
					String string2 = arraylist1.get(k);
					
					for(int l = k + 1; l < arraylist1.size(); l ++) {
						ArrayList<String> temp = new ArrayList<String>();
						String string3 = arraylist1.get(l);

						temp.add(string1);
						temp.add(string2);
						temp.add(string3);
						
						if(countin(filter2, temp)) {
							String string = string1 + "," + string2 + "," + string3;
							
							outputStream1.write(string.getBytes(Charset.forName("UTF-8")));
				        	outputStream1.writeBytes("\n");
						}
					}
				}
			}	
		}
		
		
		@SuppressWarnings("deprecation")
		Job job = new Job(config, "Read a file");
		
		FileSystem fs = FileSystem.get(config);
		
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
        job.setMapperClass(FrequentItemsetsMapper.class);
        job.setReducerClass(FrequentItemsetsReducer.class);
           
        
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/filter3.txt"));
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setJarByClass(FrequentItemsets.class);
        
        job.waitForCompletion(true);
        
        Path file3 = new Path(otherArgs[1] + "/part-r-00000");//Location of file in HDFS
		FileSystem fs3 = FileSystem.get(new Configuration());
		BufferedReader br3 = new BufferedReader(new InputStreamReader(fs3.open(file3)));
        
		Map<String, Integer> filter3 = new HashMap<String, Integer>();
		
		while(br3.ready()) {
			String string1[] = br3.readLine().split("\t");
			
			filter3.put(string1[0], Integer.parseInt(string1[1]));

		}
		
		Comparator<Map.Entry<String, Integer>> valueComparator = new Comparator<Map.Entry<String,Integer>>() {
	        @Override
	        public int compare(Entry<String, Integer> o1,
	                Entry<String, Integer> o2) {
	            return o1.getValue()-o2.getValue();
	        }
	    };

	    List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String,Integer>>(filter3.entrySet());

	    Collections.sort(list,valueComparator);

	    Path output2 = new Path(otherArgs[0] + "/final.txt");
		FileSystem fout2 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream2 = fout2.create(output2);
	    
	    for (Map.Entry<String, Integer> entry : list) {
	    	String string = entry.getKey() + ":" + entry.getValue();
	    	
	    	outputStream2.write(string.getBytes(Charset.forName("UTF-8")));
        	outputStream2.writeBytes("\n");
	    }
	}
	
	public static void countnum(Map<ArrayList<String>, Integer> count2,ArrayList<String> singer1) {
		ArrayList<String> singer2 = new ArrayList<String>();
		
		singer2.add(singer1.get(1));
		singer2.add(singer1.get(0));
		
		if(count2.containsKey(singer1)) {
			int value = count2.get(singer1);
			value = value + 1;
			
			count2.put(singer1, value);
		}else if(count2.containsKey(singer1)) {
			int value = count2.get(singer2);
			value = value + 1;
			
			count2.put(singer2, value);
		}else {
			count2.put(singer1, 1);
		}
	}
	
	public static boolean countin(ArrayList<ArrayList<String>> filter2, ArrayList<String> temp){
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		for(int i = 0; i < temp.size(); i ++){				//Initialize map = {A = 0, B = 0, C = 0}
			map.put(temp.get(i), 0);
		}
		
		for(int i = 0; i < filter2.size(); i ++) {
			ArrayList<String> temp1 = filter2.get(i);
			
			String s1 = temp1.get(0);
			String s2 = temp1.get(1);
			
			if(map.containsKey(s1) && map.containsKey(s2)) {
				int value1 = map.get(s1);
				int value2 = map.get(s2);
				
				map.put(s1, value1 + 1);
				map.put(s2, value2 + 1);
			}
		}
		
		int count = 0;
		
		for(Map.Entry<String, Integer> entry1 : map.entrySet()) {
			int value = entry1.getValue();
			
			if(value >= 2) {
				count++;
			}
		}
		
		if(count==map.size()) {
			return(true);
		}else {
			return(false);
		}
	}
	
}
