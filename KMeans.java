package edu.stanford.cs246.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.io.Files;

public class KMeans extends Configured implements Tool {
	
	public static final int MAX_ITER = 20;
	public static final int NUM_CLUSTERS = 10;
	public static final int DIMENSIONS = 58;
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new KMeans(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      Path inputDir = new Path(args[0]);
      String outputDir = args[1];
      String centroidFile = args[2];
      String costType = args[3];
      
      PrintWriter costFile = new PrintWriter(new BufferedWriter(new FileWriter("cost.txt", true)));

      for(int i = 0; i < MAX_ITER; i++) {
    	  
    	  long costL2 = 0;
    	  long costL1 = 0;
    	  
    	  Job job = Job.getInstance();
    	  
    	  Configuration conf = job.getConfiguration();
		  conf.set("costType", costType);
    	  
          job.setJarByClass(KMeans.class);
          job.setMapOutputKeyClass(IntWritable.class);
          job.setMapOutputValueClass(Text.class);
          job.setOutputKeyClass(NullWritable.class);
          job.setOutputValueClass(Text.class);

          job.setMapperClass(Map.class);
          job.setReducerClass(Reduce.class);

          job.setInputFormatClass(TextInputFormat.class);
          job.setOutputFormatClass(TextOutputFormat.class);
          
          //Add to Distributed Cache
          URI centroidURI = new URI(centroidFile);
          job.addCacheFile(centroidURI);
          
          FileInputFormat.addInputPath(job, inputDir);
          FileOutputFormat.setOutputPath(job, new Path(outputDir + Integer.toString(i)));
          job.waitForCompletion(true);
          
          long cost = job.getCounters().findCounter("Cost-Counter", "cost").getValue();
          
          double costDouble = ((double)cost)/10000;
       
          costFile.println(Integer.toString(i) + "," + Double.toString(costDouble));

		  centroidFile =  outputDir + Integer.toString(i) + "/part-r-00000";
      }
      
      costFile.close();
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
      private Text valueMap = new Text();
      private IntWritable keyMap = new IntWritable();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  double[] docVector = new double[DIMENSIONS];  
    	  
    	  String[] entries = value.toString().split(" ");
    	  for(int i = 0; i < DIMENSIONS; i++){
    		  docVector[i] = Double.parseDouble(entries[i]);
    	  }
    	  
    	  //Accessing the file storing the centroids in the Distributed Cache
    	  URI[] uriCentroids = context.getCacheFiles();   	  
    	  BufferedReader infile = new BufferedReader(new FileReader(uriCentroids[0].toString()));
    	  Configuration conf = context.getConfiguration();
    	  String costType = conf.get("costType");

          double[] distCentroids = new double[NUM_CLUSTERS];
       
          String str;
          int centroid = 0;
          while((str = infile.readLine()) != null && centroid < NUM_CLUSTERS){
        	  String[] centroidVector = str.split(" ");
        	  double error = 0;
        	  for(int i = 0; i < DIMENSIONS; i++){
        		  if(costType.equals("l2")){
        			  error += (docVector[i] - Double.parseDouble(centroidVector[i])) * (docVector[i] - Double.parseDouble(centroidVector[i]));
        		  } else if(costType.equals("l1")){
        			  error += Math.abs(docVector[i] - Double.parseDouble(centroidVector[i]));
        		  }
        		  
        	  }
        	  distCentroids[centroid] = error;
        	  centroid++;
          }
          infile.close();
          
          int minIndex = minIndex(distCentroids);
          
          keyMap.set(minIndex);
          valueMap.set(value);
          context.write(keyMap, valueMap);
          
          context.getCounter("Cost-Counter", "cost").increment((long)distCentroids[minIndex]*10000);
          
          //keyMap.set(NUM_CLUSTERS);
          //valueMap.set(Double.toString(distCentroids[minIndex]));
          //context.write(keyMap, valueMap);
      }
   }

   public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {
	   private Text valueReducer = new Text();
	   private NullWritable keyReducer;
	   
      @Override
      public void reduce(IntWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

    	 double[] newCentroid = new double[DIMENSIONS];
    	 
    	 if(key.get() == NUM_CLUSTERS){
    		 int sum = 0;
             for (Text val : values) {
                sum += Double.parseDouble(val.toString());
             }
             valueReducer.set(Double.toString(sum));
             context.write(keyReducer, valueReducer);
    	 }
    	 
    	 System.out.println(key.toString() + "reduce");
    	 
    	 int counter = 0;
    	 String[] entries;
    	 for(Text val: values){
    		 entries = val.toString().split(" ");
    		 
    		 for(int i = 0; i < DIMENSIONS; i++){
        			 newCentroid[i] += Double.parseDouble(entries[i]);
        	 }
    		 counter++;
    	 }
    	 
    	 String[] newCentroidStr = new String[DIMENSIONS];
    	 
    	 if(counter != 0){
			 for (int i = 0; i < DIMENSIONS; i++) {
		         newCentroid[i] = newCentroid[i]/counter;
		         newCentroidStr[i] = String.format("%.5f", newCentroid[i]);
		     }
    	 } else {
    		 System.out.println("Souci!");
    	 }
    	 
    	 //valueReducer.set(Arrays.toString(newCentroid).replace("[", "").replace("]", "").replace(",", ""));
    	 valueReducer.set(Arrays.toString(newCentroidStr).replace("[", "").replace("]", "").replace(",", ""));
    	 context.write(keyReducer, valueReducer);
         }
      }

	public static int minIndex(double... ds) {
	    int idx = -1;
	    double d= Double.POSITIVE_INFINITY;
	    for(int i = 0; i < ds.length; i++){
	    	if(ds[i] < d) {
	            d = ds[i];
	            idx = i;
	        }
	    } 
	    return idx;
	}

}
