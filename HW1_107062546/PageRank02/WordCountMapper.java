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


public class WordCountMapper extends Mapper<Object, Text, Text, Text>{

	private double outlink;
	
	public void map(Object key, Text values, Context context)throws IOException, InterruptedException 
	{
		StringTokenizer itr = new StringTokenizer(values.toString());
		double rank = 0;
		Text fromnode = new Text();
		ArrayList<String> tonodelist = new ArrayList<String>();
		String tonode = new String("t ");
		
		if(itr.hasMoreTokens()) {
			fromnode.set(itr.nextToken());
			rank = Double.parseDouble(itr.nextToken());
		}

		
		while(itr.hasMoreTokens()){
			tonodelist.add(itr.nextToken());
		}
		
		outlink = rank / tonodelist.size();
		
		for(int i = 0; i<tonodelist.size(); i++){
			Text keys = new Text(tonodelist.get(i));
			Text value = new Text("r " + outlink);
			
			context.write(keys, value);	
		}
		
		for(int i = 0; i<tonodelist.size(); i++){
			tonode = tonode + " " + tonodelist.get(i);
		}
		
		context.write(fromnode, new Text(tonode));
	}
	
}
