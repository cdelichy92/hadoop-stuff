//People You Might Know: MapReduce Implementation

package edu.stanford.cs246.PYMK;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class pymk extends Configured implements Tool {
	
	public static final String OUTPUT_PATH = "intermediate_output";
	public static final int NUM_RECOMMENDATIONS = 10;

	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new pymk(), args);
	      
	      System.exit(res);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      
	      Job job1 = new Job(getConf(), "pymk");
	      job1.setJarByClass(pymk.class);
	      job1.setMapOutputKeyClass(IntPrWritable.class);
	      job1.setMapOutputValueClass(BooleanWritable.class);
	      
	      job1.setOutputKeyClass(Text.class);
	      job1.setOutputValueClass(IntWritable.class);

	      job1.setMapperClass(Map.class);
	      job1.setReducerClass(Reduce.class);

	      job1.setInputFormatClass(TextInputFormat.class);
	      job1.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job1, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

	      job1.waitForCompletion(true);
	      
	      Job job2 = new Job(getConf(), "pymk");
	      job2.setJarByClass(pymk.class);
	      job2.setMapOutputKeyClass(IntWritable.class);
	      job2.setMapOutputValueClass(Text.class);
	      job2.setOutputKeyClass(IntWritable.class);
	      job2.setOutputValueClass(Text.class);

	      job2.setMapperClass(Map2.class);
	      job2.setReducerClass(Reduce2.class);

	      job2.setInputFormatClass(TextInputFormat.class);
	      job2.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	      FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	      job2.waitForCompletion(true);
	      
	      return 0;
	   }
	   
	   public static class Map extends Mapper<LongWritable, Text, IntPrWritable, BooleanWritable> {
	      private BooleanWritable valueMap = new BooleanWritable();
	      private IntPrWritable keyMap = new IntPrWritable();
	      
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	 
	    	 String[] entries = value.toString().split("\\t");
	    	 int i = Integer.parseInt(entries[0]);
	    	 String[] friends = entries[1].split(",");
	    	 keyMap.set(i, i);
	    	 valueMap.set(false);
	    	 context.write(keyMap, valueMap);
        		 
    		 for (String friend : friends) {
	        	 int j = Integer.parseInt(friend);
	        	 if(i < j) {
	        		 keyMap.set(i, j);
	        	 } else {
	        		 keyMap.set(j, i);
	        	 }
	        	 valueMap.set(false);
	        	 context.write(keyMap, valueMap);
    		 
        		 for (String friend2 : friends) {
        			 int k = Integer.parseInt(friend2);
        			 if(j < k){
        				 keyMap.set(j, k);
        	        	 valueMap.set(true);
        	        	 context.write(keyMap, valueMap);
        			 }
        		 }
    	 	}   
         }
      }

		public static class Reduce extends Reducer<IntPrWritable, BooleanWritable, Text, IntWritable> {
			private IntWritable valueReducer = new IntWritable();
			
		  @Override
	      public void reduce(IntPrWritable key, Iterable<BooleanWritable> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	 int counter = 0;
	    	 boolean stop = false;
	    	 
	    	 for (BooleanWritable val : values){
	    		 if (val.get()){
	    			 counter++;
	    		 } else {
	    			 stop = true;
	    			 break;
	    		 }
	    	 }
	    	 if (!stop){
	    		 valueReducer.set(counter);
		    	 context.write(new Text(key.int1+","+key.int2), valueReducer);
	    	 } else {
	    		 valueReducer.set(0);
		    	 context.write(new Text(key.int1+","+key.int2), valueReducer);
	    	 }
	      }
	   }
		
		public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		      private IntWritable keyMap = new IntWritable();
		      
		      @Override
		      public void map(LongWritable key, Text value, Context context)
		              throws IOException, InterruptedException {
		    	 
		    	   String[] entries = value.toString().split("\\t");
		    	   int i = Integer.parseInt(entries[0].split(",")[0]);
		    	   int j = Integer.parseInt(entries[0].split(",")[1]);
		    	   int num = Integer.parseInt(entries[1]);
		    	   
		    	   keyMap.set(i);
		           context.write(keyMap, new Text(num+","+j));
		           keyMap.set(j);
		           context.write(keyMap, new Text(num+","+i));
		         }
		}
		
		public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text> {
		      @Override
		      public void reduce(IntWritable key, Iterable<Text> values, Context context)
		              throws IOException, InterruptedException {
		          
		    	  Comparator<IntPrWritable> comparator = new IntPrComparator();
		          PriorityQueue<IntPrWritable> priorityQueue = new PriorityQueue<IntPrWritable>(10, comparator);
		          
		    	  for(Text val : values){
		    		  String[] entries = val.toString().split(",");
		 	    	  int int1 = Integer.parseInt(entries[0]);
		 	    	  int int2 = Integer.parseInt(entries[1]);
		 	    	  if(int1 != 0){
			    		  IntPrWritable val2 = new IntPrWritable();
			    		  val2.set(int1, int2);
			    		  priorityQueue.add(val2);
		 	    	  }
		    	  }
		    	  String outputString = "";
		    	  
		    	  for(int i = 0; i < NUM_RECOMMENDATIONS; i++){
		    		  if(priorityQueue.peek() != null){
		    			  outputString += priorityQueue.poll().int2 + ",";
		    		  }
		    	  }
		    	  if(outputString.length() > 0){
		    		  outputString = outputString.substring(0, outputString.length()-1);
		    	  }
		    	  context.write(key, new Text(outputString));
		      }
		}
}

class IntPrWritable implements WritableComparable<IntPrWritable>{

    public int int1;
    public int int2;
    
    void set(int int1Value, int int2Value){
    	this.int1 = int1Value;
    	this.int2 = int2Value;
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeInt(int1);
      out.writeInt(int2);
    }
    
    public void readFields(DataInput in) throws IOException {
      int1 = in.readInt();
      int2 = in.readInt();
    }
    
    public int compareTo(IntPrWritable o) {
    	if(this.int1 > o.int1){
    		return 1;
    	} else if(this.int1 < o.int1){
    		return -1;
    	} else {
    		if(this.int2 < o.int2){
    			return 1;
    		} else if(this.int2 > o.int2){
    			return -1;
    		} else {
    			return 0;
    		}
    	}
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + int1;
        result = prime * result + (int) (int2 ^ (int2 >>> 32));
        return result;
      }
  }


class BoolIntPrWritable implements Writable {

    public boolean bool;
    public int integer;
    
    void set(boolean boolValue, int integerValue){
    	this.bool = boolValue;
    	this.integer = integerValue;
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(bool);
      out.writeInt(integer);
    }
    
    public void readFields(DataInput in) throws IOException {
      bool = in.readBoolean();
      integer = in.readInt();
    }
  }

class IntPrComparator implements Comparator<IntPrWritable>{
	
	public int compare(IntPrWritable a, IntPrWritable b){
		 return b.compareTo(a);
	}
}