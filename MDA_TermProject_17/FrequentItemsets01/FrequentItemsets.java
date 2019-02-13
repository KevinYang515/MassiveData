import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
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
		
        
        String file = otherArgs[0] + "/total2.txt";
        Path in = new Path(file);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(in)));
		
        Map<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();	//{user = [singer1, singer2]}
         	
        
		while (br.ready()) {
			ArrayList<String> temp = new ArrayList<String>();
			
			String tem[] = br.readLine().split(",");
			for(int i = 0; i < tem.length; i ++) {
				temp.add(tem[i]);
			}
			
			if(map.containsKey(temp.get(0))) {
				ArrayList<String> arr = new ArrayList<String>();
				arr = map.get(temp.get(0));
				
				String singer = temp.get(8);
				
				if(!arr.contains(singer)) {
					arr.add(singer);
				}
				
			}else {
				ArrayList<String> arr = new ArrayList<String>();
				String singer = temp.get(8);
				
				arr.add(singer);
				
				map.put(temp.get(0), arr);
			}
		}
		
		String output1 = otherArgs[0] + "/output-01.txt";
		
		Path out = new Path(output1);
		FileSystem fs0 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream = fs0.create(out);
        
        for(Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
        	ArrayList<String> arraylist1 = entry.getValue();
        	String string = "";
        	for(int i = 0; i < arraylist1.size(); i ++) {
        		if(i < arraylist1.size() - 1) {
        			string = string + arraylist1.get(i) + ",";
        		}else {
        			string = string + arraylist1.get(i);
        		}
        		
        	}
        	outputStream.write(string.getBytes(Charset.forName("UTF-8")));
        	outputStream.writeBytes("\n");
        }
		

		
		Job job = Job.getInstance(config, "FrequentItemsets");
        job.setJarByClass(FrequentItemsets.class);
        
        if (fs.exists(new Path(args[1])))
            fs.delete(new Path(args[1]), true);
        
        job.setMapperClass(FrequentItemsetsMapper.class);
        job.setReducerClass(FrequentItemsetsReducer.class);
           
        job.setMapOutputKeyClass(Text.class);
        
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputValueClass(Text.class);
        
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/output-1.txt"));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        job.waitForCompletion(true);
	}
}
