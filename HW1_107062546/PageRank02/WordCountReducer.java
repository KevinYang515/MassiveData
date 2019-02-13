import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCountReducer extends Reducer<Text, Text, Text, Text>{
	private static final long nodenum = 10876;
	private static final double beta = 0.8;
	
	private Text value = new Text();
	private Text key = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		double outranksum = 0;
		String tonodearrlst = new String("");
		
		for(Text val:values){
			StringTokenizer itr = new StringTokenizer(val.toString());
			
			String temp = new String(itr.nextToken());
			if( temp.equals("r") ) { //r : outrank
				outranksum = outranksum + Double.parseDouble(itr.nextToken());
			}
			else if(temp.equals("t")){ //t : tonode
				while(itr.hasMoreTokens()){
					tonodearrlst = tonodearrlst + " " + itr.nextToken();
				}
			}
        }
		
		double pagerank = beta * outranksum + (1-beta)/nodenum;
		
		key.set(key + " " + pagerank);
		value.set(tonodearrlst);
		context.write(key, value);
	}
}
