import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;


public class Minhashing {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Map<Integer, ArrayList<String>> map = new HashMap<Integer, ArrayList<String>>();
			
		for(int i = 1; i <= 50; i++) {
			String inputpath = otherArgs[0];
			
			if(i<10) {
				inputpath = inputpath + "/00"+i +".txt";
			}else {
				inputpath = inputpath + "/0"+i +".txt";
			}
			
			System.out.println("\n" + inputpath + "\n");
			
			Path in=new Path(inputpath);//Location of file in HDFS
	        FileSystem fs = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(in)));
	       
	        String line;
	        line = br.readLine();
	        
	        String a[] = line.split(":");
	        line=br.readLine();
	        
	        ArrayList<String> list = new ArrayList<String>();
	        
	        while (line != null){
	            list.add(line);
	            
	            line=br.readLine();
	        }
	       
	        map.put(Integer.parseInt(a[1]), list);
	   
	        fs.close();
	        
		}
		
		String total[] = new String[51];
		
		for(int filenum = 1; filenum <=50; filenum++) {
			String outputpath = otherArgs[1];
			
			String filepath = "";
			
			if(filenum< 10) {
				filepath = "/Minhashing00";
			}else {
				filepath = "/Minhashing0";
			}
			
			Path out = new Path(outputpath + filepath + filenum + ".txt");
			FileSystem fs = FileSystem.get(new Configuration());
	        FSDataOutputStream outputStream=fs.create(out);
			
			Map<Integer, Map<Integer, String>> orimap = new HashMap<Integer, Map<Integer, String>>();
			Map<Integer, Map<Integer, String>> doublemap = new HashMap<Integer, Map<Integer, String>>();
			
			for(int i = 1; i <= 50; i++) {
				Map<Integer, String> map1 = new HashMap<Integer, String>();
				
				doublemap.put(i, permute(map.get(i), map1));
				orimap.put(i, map1);
			}
			
			int a[][] = new int[doublemap.get(filenum).size()][doublemap.size()];
			
			
			for(int i = 0; i < doublemap.get(filenum).size(); i++) {
				
				String word = doublemap.get(filenum).get(i);
				System.out.print(i + word);
				outputStream.writeBytes(word);
				
				for(int j = 1; j <= doublemap.size(); j++) {
					
					int ans = 2;
					if(doublemap.get(j).containsValue(word)) {
						ans = 1;
					}else {
						ans = 0;
					}
					
//					System.out.print(ans);
					a[i][j-1] = ans;
					outputStream.writeBytes(ans + " ");	
				}
				
				System.out.println();
				outputStream.write('\n');
			}
			
			
			for(int i = 0; i < a.length;i++) {
				for(int j = 0; j < a[i].length; j++) {
					System.out.print(a[i][j]);
				}
				
				System.out.println();
			}
			
			int b[] = new int[doublemap.size()];
			
			for(int j = 0; j < a[0].length; j++) {
				int temp = 100000;
				
				for(int i = 0; i < a.length; i++) {
					if(a[i][j] == 1) {
						
						if(getAllKeysForValue(orimap.get(filenum), doublemap.get(filenum).get(i)) < temp) {
							
							temp = getAllKeysForValue(orimap.get(filenum), doublemap.get(filenum).get(i));
						}
					}
				}
				
				b[j] = temp;
				if (b[j] == 100000) {
					b[j] = 0;
				}
			}
			
			String stringtotal = filenum + ",";
	        
			for(int i = 0; i < b.length; i++) {
				
				stringtotal = stringtotal + b[i];
				
				if(i < b.length - 1) {
					stringtotal = stringtotal + ",";
				}
				
				System.out.print(b[i] +",");
			}
				
			total[filenum] = stringtotal;
			fs.close();
			
		}
		
		Path out1 = new Path(otherArgs[1] + "/Total.txt");
		FileSystem fs1 = FileSystem.get(new Configuration());
        FSDataOutputStream outputStream1=fs1.create(out1);
         
        
        for(int i = 1; i <= 50; i++) {
        	System.out.println(total[i]);
        	outputStream1.writeBytes(total[i]);
        	outputStream1.write('\n');
        }
		
		fs1.close();
		
	}	
	

	public static int countnum(String path) throws IOException {
		int count = 0;
		
		Path in=new Path(path);//Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(in)));
		
        String string;
        string = br.readLine();
        
        while(string != null) {
        	count++;
        	br.readLine();
        }
        
		return(count);
	}
	
	public static Map<Integer, String> permute(ArrayList<String> string, Map<Integer, String> orimap) {
		Random ran = new Random();
        int a = ran.nextInt(50) + 1;
        int b = ran.nextInt(50) + 1;
        Map<Integer, String> map1 = new HashMap<Integer,String>();
        Map<Integer, String> map2 = new HashMap<Integer,String>();
        int p = generateprime(string.size());
        
        for(int i = 0; i < string.size(); i++) {
        	orimap.put(i, string.get(i));
        }
        
        for(int i = 0; i < string.size(); i ++) {
        	 map1.put((a * i + b)% p, string.get(i));
        }
        
        int count = 0; 
        
        for(int i = 0; i < map1.size(); i++) {
        	
        	if(map1.get(i) != null) {
        		map2.put(count, map1.get(i));
        		count++;
        	}
        }
        
        return(map2);
	}
	
	public static int generateprime(int num) {
		int prime = 0;
		
		for(int i = num + 1; i < num + 100; i ++) {
			if(isPrimeBruteForce(i)) {
				prime = i;
				break;
			}
		}
		
		return(prime);
	}
	
	public static boolean isPrimeBruteForce(int number) {
	    
		for (int i = 2; i < number; i++) {
	        
			if (number % i == 0) {
	            return false;
	        }
	    }
		
	    return true;
	}
	
	public static int getAllKeysForValue(Map<Integer, String> mapOfWords, String string) 
	{
		if(mapOfWords.containsValue(string)){
			
			for (Map.Entry<Integer, String> entry : mapOfWords.entrySet()){
				
				if (entry.getValue().equals(string)){
					
					return(entry.getKey());
				}
			}
			
			System.out.println("Found");
			return 500;
		}else {
			
			System.out.println("Not Found");
			return -1;
		}
	}
}
