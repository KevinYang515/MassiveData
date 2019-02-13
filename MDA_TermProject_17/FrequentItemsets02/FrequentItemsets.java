import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;

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
		
		Path file2 = new Path(otherArgs[0] + "/part-r-00001");//Location of file in HDFS
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
		
		
		ArrayList<String> fliter1 = new ArrayList<String>();
		
		while (br2.ready()){
			String line[] = br2.readLine().split("\t");
			
			fliter1.add(line[0]);
		}
		
		fs1.close();
		fs2.close();
		
		Path output1 = new Path(otherArgs[0] + "/freq2.txt");
		FileSystem fout1 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream1 = fout1.create(output1);
		
		for(int i = 0; i < total.size(); i ++) {
			ArrayList<String> arraylist1 = total.get(i);
			
			for(int j = 0; j < arraylist1.size() - 1; j ++) {
				String string1 = arraylist1.get(j);
				
				for(int k = j + 1; k < arraylist1.size(); k ++) {
					
					String string2 = arraylist1.get(k);
					
					if(fliter1.contains(string1) && fliter1.contains(string2)) {
						String string = string1 + "," + string2;
						
						outputStream1.write(string.getBytes(Charset.forName("UTF-8")));
			        	outputStream1.writeBytes("\n");
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
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/filter2.txt"));
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setJarByClass(FrequentItemsets.class);
        
        job.waitForCompletion(true);
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
	
}
