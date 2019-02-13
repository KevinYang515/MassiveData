import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class Similarity {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Map<String, Double> map = new TreeMap<String, Double>();
		
		String inputpath = otherArgs[0];
		String outputpath = otherArgs[1];
		
		int [][] signa = new int[50][50]; 
		
		Path in=new Path(inputpath + "/Total.txt");//Location of file in HDFS
	    FileSystem fs = FileSystem.get(new Configuration());
	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(in)));
	    
	    for(int i = 0; i < 50; i++) {
	    	String string = "";
	    	string = br.readLine();
	    	
	    	String total[] = string.split(",");
	    	
	    	for(int j = 0; j < total.length ; j++) {
	    		if(j < total.length - 1) {
	    			signa[i][j] = Integer.parseInt(total[j + 1]);
	    			
	    		}
	    	}	
	   }
	    
	   
	    Path in2 =new Path(outputpath + "/part-r-00000");//Location of file in HDFS
	    FileSystem fs2 = FileSystem.get(new Configuration());
	    BufferedReader br2=new BufferedReader(new InputStreamReader(fs2.open(in2)));
	    
	    for(int n = 0; n < 25; n++) {
	    	String temp[] = br2.readLine().split("\t");
	    	
		    String temp1[] = temp[1].split(";");
		    
		    for(int i = 0; i < temp1.length ; i ++) {
		    	String temp2[] = temp1[i].split(":");
		    	String temp3[] = temp2[1].split(",");
		    	
		    	for(int j = 0; j < temp3.length ; j ++) {
		    		
		    		for(int k = j + 1; k < temp3.length; k++) {
		    			
		    			double sim = countsim(temp3[j], temp3[k], signa);
		    			String set = (Integer.parseInt(temp3[j]) + 1) + "," + (Integer.parseInt(temp3[k]) + 1);
		    			
		    			if(map.get(set) == null) {
		    				map.put(set, sim);
		    				
		    			}else if(map.get(set) != null && sim > map.get(set)) {
		    				map.put(set, sim);
		    				
		    			}
		    		}
		    	}
		    }
	    }
	    
	    Set<Entry<String, Double>> set = map.entrySet();
	    List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
	    
	    Collections.sort( list, new Comparator<Map.Entry<String, Double>>()
	    {
	      public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )
	      {
	        int result = (o2.getValue()).compareTo( o1.getValue() );
	        
	        if (result != 0) {
	          return result;
	          
	        } else {
	          return o1.getKey().compareTo(o2.getKey());
	          
	        }
	      }
	    } );
	    
	    
	    Path out = new Path(outputpath + "/answer.txt");
		FileSystem fs3 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream=fs3.create(out);
	    
        for(int i = 0; i < 10; i++) {
        	Entry<String, Double> a = list.get(i);
        	outputStream.writeBytes(a.toString());  
        	outputStream.write('\n');
        }
	}
	
	public static double countsim(String a, String b, int siga[][]) {
		int count = 0;
		double sim = 0.0;
		
		for(int i = 0; i < 50; i++) {
			
			if(siga[i][Integer.parseInt(a)] == siga[i][Integer.parseInt(b)]) {
				count++;
			}
		}
		
		sim = count / 50.0;
		
		return(sim);
	}
}
