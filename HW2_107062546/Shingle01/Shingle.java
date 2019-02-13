import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;


public class Shingle {
	public int aw;
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		ArrayList<String> string1 = new ArrayList<String>();
			
		for(int i = 1; i < 51 ; i++) {
			String inputpath = otherArgs[0];
			String outputpath = otherArgs[1];
			
			if(i<10) {
				inputpath = inputpath + "/00" + i + ".txt";
				outputpath = outputpath + "/00" + i + ".txt";
			}
			else {
				inputpath = inputpath + "/0" + i + ".txt";
				outputpath = outputpath + "/0" + i + ".txt";
			}
			
			Path in=new Path(inputpath);//Location of file in HDFS
	        FileSystem fs = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(in)));
	        
	        String line;
	        line=br.readLine();
	        
	        Path out = new Path(outputpath);
	        FSDataOutputStream outputStream=fs.create(out);
	        
	        String file = "File:0";
        	
	        if(i < 10) {
        		outputStream.writeBytes(file + "0" + i);
        		outputStream.write('\n');
        		
        	}else {
        		outputStream.writeBytes(file + i );
        		outputStream.write('\n');
        		
        	}
	        
	        while (line != null){
	            line = line.replaceAll("\"", "");
	        	System.out.println(line);
	          
	        	string1 = kshingle(line);
	            
	        	for(int j = 0; j< string1.size() ; j++) {
	            	outputStream.writeBytes(string1.get(j));
	            	outputStream.write('\n');
	            	System.out.println(string1.get(j));
	            }
	            
	            line=br.readLine();
	        }
	        
	        outputStream.close();
		}
	}
	
	public static ArrayList<String> kshingle(String string) {
		ArrayList<String> str = new ArrayList<String>();
		str = Shingle(split(string), 3);
		
		return(str);
	}
	
	public static ArrayList<String> split(String string1) {
		ArrayList<String> str = new ArrayList<String>();
		String a[] = string1.split(" ");
	
		for(int i = 0; i < a.length; i++) {
			str.add(a[i]);
		}
		
		return(str);
	}
	
	public static ArrayList<String> Shingle(ArrayList<String> string1, int num) {
		ArrayList<String> str = new ArrayList<String>();
		
		for(int i = 0; i + num <= string1.size(); i++) {
			String string = "";
			
			for(int j = i; j < i + num; j++) {
				if(j == i) {
					string = string1.get(j);
				}else {
					string = string + " " + string1.get(j);
				}
			}
			str.add(string);
		}
		
		return(str);
	}
	
}
