import static java.lang.Math.pow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class kmeans {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, NumberFormatException, IllegalArgumentException, IllegalStateException {

		for(int iteration = 0; iteration < 20;iteration ++)
		{
			Configuration config = new Configuration();
			String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
	
			String inputpath = otherArgs[0];
			
			String file = "";
			String file1 = "";
			
			if(iteration == 0) {
				file = inputpath + "/data.txt";
				file1 = inputpath + "/c1.txt";
			}else {
				file = inputpath + "/data.txt";
				file1 = inputpath + "/output" + iteration +"/part-r-00000";
			}
			
			
			Map<Integer, String> map = new HashMap<Integer, String>();
			Map<Integer, ArrayList<Double>> mapc1 = new HashMap<Integer, ArrayList<Double>>();
			Map<Integer, ArrayList<Double>> map1 = new HashMap<Integer, ArrayList<Double>>();
			
			Path in = new Path(file);//Location of file in HDFS
	        FileSystem fs = FileSystem.get(new Configuration());
	        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(in)));
			
			Path in1 = new Path(file1);//Location of file in HDFS
	        FileSystem fs1 = FileSystem.get(new Configuration());
	        BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(in1)));
	
	        Path in2 = new Path(inputpath + "/c1.txt");
	        FileSystem fs2 = FileSystem.get(new Configuration());
	        BufferedReader br2 = new BufferedReader(new InputStreamReader(fs2.open(in2)));
	        
	        int num = 0;
			while (br.ready()) {
				String line = br.readLine();
				map.put(num, line);
				num++;
			}
			
			int num1 = 0;
			if(iteration != 0) {
				
				while (br1.ready()) {
					String line1 = br1.readLine();
					String[] templine = line1.split("\t");
					String[] number1 = templine[1].split(" ");
					
					ArrayList<Double> array1 = new ArrayList<Double>();
					
					for(int i = 0; i < number1.length ; i ++) {
						array1.add(Double.parseDouble(number1[i]));
					}
					
					map1.put(Integer.parseInt(templine[0]), array1);
					num1 ++;
				}
			}else {
				while (br1.ready()) {
					String line1 = br1.readLine();
					String[] number1 = line1.split(" ");
					
					ArrayList<Double> array1 = new ArrayList<Double>();
					
					for(int i = 0; i < number1.length ; i ++) {
						array1.add(Double.parseDouble(number1[i]));
					}
					
					map1.put(num1, array1);
					num1 ++;
				}
			}
			
			
			int num2 = 0;
			while (br2.ready()) {
				String line2 = br2.readLine();
				String[] number2 = line2.split(" ");
				
				ArrayList<Double> array2 = new ArrayList<Double>();
				
				for(int i = 0; i < number2.length ; i ++) {
					array2.add(Double.parseDouble(number2[i]));
				}
				
				mapc1.put(num2, array2);
				num2 ++;
			}
			
			for(int i = 0; i < num2; i++) {
				if(map1.get(i) == null) {
					map1.put(i, mapc1.get(i));
				}
			}
			
			
			fs.close();
			fs1.close();
			fs2.close();
			
			ArrayList<ArrayList<Double>> dat = new ArrayList<ArrayList<Double>>();
			
			for(int i = 0; i < num; i ++) {
				 String string[] = map.get(i).split(" ");
				 ArrayList<Double> data = new ArrayList<Double>();
				 
				 for(int j = 0; j < string.length; j ++) {
					 data.add(Double.parseDouble(string[j]));
				 }
				 dat.add(data);
			}
			
			ArrayList<ArrayList<Double>> compa = new ArrayList<ArrayList<Double>>(); 
			compa = distance(dat, map1, num1);
			
			ArrayList<Integer> comp = new ArrayList<Integer>();
			
			comp = comparison(compa);
	
			
			String output1 = inputpath + "/output" + (iteration + 1) + ".txt";
			
			Path out = new Path(output1);
			FileSystem fs0 = FileSystem.get(new Configuration());
	        FSDataOutputStream outputStream=fs0.create(out);
	        
	        for(int i = 0; i < comp.size(); i++){
	        	outputStream.writeBytes(comp.get(i)+" ");
	        	outputStream.writeBytes(map.get(i));
	        	outputStream.write('\n');
	        }
	        
	        
	        outputStream.close();
			
	        Job job = Job.getInstance(config, "Kmeans");
	        job.setJarByClass(kmeans.class);
	        
	        job.setMapperClass(kmeansMapper.class);
	        job.setReducerClass(kmeansReducer.class);
	           
	        job.setMapOutputKeyClass(Text.class);
	        
	        job.setMapOutputValueClass(Text.class);
	        
	        job.setOutputValueClass(Text.class);
	        
	        
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/output" + (iteration + 1) + ".txt"));
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0] + "/output" + (iteration + 1) + "/"));
	        
	        job.waitForCompletion(true);
		}
		
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		
		String finaloutpath = otherArgs[0];
		
		Path in = new Path(finaloutpath + "/output20/part-r-00000");//Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(in)));
        
        Path in1 = new Path(finaloutpath + "/c1.txt");//Location of file in HDFS
        FileSystem fs1 = FileSystem.get(new Configuration());
        BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(in1)));
        
        
        int num = 0;
        Map<Integer, ArrayList<Double>> map = new HashMap<Integer, ArrayList<Double>>();
        Map<Integer, ArrayList<Double>> mapc1 = new HashMap<Integer, ArrayList<Double>>();
        
        while (br.ready()) {
			String line1 = br.readLine();
			String[] templine = line1.split("\t");
			String[] number1 = templine[1].split(" ");
			
			ArrayList<Double> array1 = new ArrayList<Double>();
			
			for(int i = 0; i < number1.length ; i ++) {
				array1.add(Double.parseDouble(number1[i]));
			}
			
			map.put(num, array1);
			num ++;
		}
        
        int num1 = 0;
		while (br1.ready()) {
			String line1 = br1.readLine();
			String[] number1 = line1.split(" ");
			
			ArrayList<Double> array1 = new ArrayList<Double>();
			
			for(int i = 0; i < number1.length ; i ++) {
				array1.add(Double.parseDouble(number1[i]));
			}
			
			mapc1.put(num1, array1);
			num1 ++;
		}
		
		for(int i = 0; i < num1; i++) {
			if(map.get(i) == null) {
				map.put(i, mapc1.get(i));
			}
		}
		
		Path out = new Path(finaloutpath + "/finalanswer.txt");
		FileSystem fs0 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream = fs0.create(out);
        
        for(int i = 0; i < num1; i ++) {
        	for(int j = i + 1; j < num1; j ++) {
        		double dis = distance(map.get(i), map.get(j));
        		String string1 = i + "-" + j + ":" + dis;
        		outputStream.writeBytes(string1);
        		outputStream.write('\n');
        	}
        }
		
        
	}
	
	public static ArrayList<ArrayList<Double>> distance(ArrayList<ArrayList<Double>> dat, Map<Integer, ArrayList<Double>> map1, int num1) {
		ArrayList<ArrayList<Double>> finalans = new ArrayList<ArrayList<Double>>();
		
		for(int i = 0; i < num1; i ++) {
			ArrayList<Double> array = new ArrayList<Double>();
			
			for(int k = 0; k < dat.size(); k++) {
				double total = 0;
				for(int j = 0; j < dat.get(i).size(); j ++) {
					double temp = pow( dat.get(k).get(j) - map1.get(i).get(j), 2);
					total = total + temp;
				}
				array.add(total);
			}
			finalans.add(array);
		}
		
		return(finalans);
	}
	
	public static ArrayList<Integer> comparison(ArrayList<ArrayList<Double>> compa) {
		ArrayList<Integer> comp = new ArrayList<Integer>();
		
		for(int i = 0; i < compa.get(0).size(); i ++) {
			double min = 1000000000.0;
			int mini = -1;
			
			for(int j = 0; j < compa.size(); j ++) {
				if(min >= compa.get(j).get(i)) {
					min = compa.get(j).get(i);
					mini = j;
				}
			}
			
			comp.add(mini);
		}
		return(comp);
	}
	
	public static double distance(ArrayList<Double> list1, ArrayList<Double> list2) {
		double total = 0.0;
		
		for(int i = 0; i < list1.size(); i ++) {
			double temp = pow((list1.get(i) - list2.get(i)), 2);
			total = total + temp;
		}
		
		return(total);
	}
}
