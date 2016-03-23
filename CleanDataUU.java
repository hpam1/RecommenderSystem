package com.cloud.project.cleanup;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class CleanDataUU extends Configured implements Tool {
	public static void main( String[] args) throws  Exception {
	      int res  = ToolRunner .run( new CleanDataUU(), args);
	      System .exit(res);
	   }
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(),"clean up task");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job,new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setMapperClass( Map .class);
	      job.setReducerClass( Reduce .class);
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( Text.class);

	      return job.waitForCompletion( true)  ? 0 : 1;

	}
	

	public static class Map extends Mapper<LongWritable, Text, Text , Text>{


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text, Text>.Context context)
				throws IOException, InterruptedException {
			String lineText = value.toString();
			JSONObject jObject;
			String reviewerID="";
			String itemID = "";
			String rating ="0";
			try {
					//Convert the string to a json object
				jObject = new JSONObject(value.toString());
				//get reviewer ID and item id and rating
				reviewerID=(String) jObject.get("reviewerID");
				itemID = (String) jObject.get("asin");
				rating =""+jObject.get("overall");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			context.write(new Text(reviewerID),new Text(itemID+":"+rating));// emit the page rank and url as key value pairs
			// this sorts the output in terms of page rank
			
		}
		
	}
	public static class Reduce extends Reducer<Text, Text,  Text, Text>{

		
		
		@Override
		protected void reduce(Text reviewerID, Iterable<Text> itemWithRatings,
				Reducer<Text, Text, Text, Text>.Context context)
						throws IOException, InterruptedException {
			String allUserRatings="";			
			for (Text itemrating : itemWithRatings) {
				allUserRatings +="\t"+itemrating;
			}
			context.write(reviewerID,new Text(allUserRatings));//
			
		}
		
		
	}

}

