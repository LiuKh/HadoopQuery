import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Mapred.TraverseGraph.TraverseMapper;
import Mapred.TraverseGraph.TraverseReducer;
import Utils.Node;


public class QueryDriver extends BaseDriver{

	/*static String topK_identifier = null;*/
	static List<String> topK_identifier = new ArrayList<String>();
	
	static class QueryTraverseMapper extends TraverseMapper {  
		  
	    public void map(Object key, Text value, Context context)  
	        throws IOException, InterruptedException {
	  
	      Node inNode = new Node(value.toString()); 
	      
	      //K=3;
	      if(topK_identifier.size() == 3){
	    	  inNode.setKthLargestScore(Double.parseDouble(topK_identifier.get(topK_identifier.size() - 1).split("&&")[0]));
	      }
	      
	      super.map(key, value, context, inNode);  
	      
	    }  
	 }  
	    
	    
	static class QueryTraverseReducer extends TraverseReducer {  	  
	  
	  public void reduce(Text key, Iterable<Text> values, Context context)  
	       throws IOException, InterruptedException {  

	      Node outNode = new Node();  

	      outNode = super.reduce(key, values, context, outNode); 

	      if (outNode.getColor() == Node.Color.GRAY){
	        context.getCounter(MoreIterations.numberOfIterations).increment(1L);  
	      }
	      
	      if(outNode.getColor() == Node.Color.RED && !isContainsId(outNode)){
	    	  topKUpdate(outNode);
	    	  System.out.println(topK_identifier.toString());
	      }
	  } 
	    
	} 

	static class MyComparator implements Comparator<String>{
		@Override
		public int compare(String s1, String s2) {
			double score1 = Double.parseDouble(s1.split("&&")[0]);
			double score2 = Double.parseDouble(s2.split("&&")[0]);
			if(score1 < score2){
				return 1;
			}

			if(score1 > score2){
				return -1;
			}

			return 0;
		}
	}
	  
	  
	  private static boolean isContainsId(Node target){
		  
		  for(String topk : topK_identifier){
			  if(target.getId().equals(topk.split("&&")[1]))
				  return true;
		  }

		  return false;
	  }

	  // K = 3
	  private static void topKUpdate(Node target){
		  
		  if(topK_identifier.size() < 3){
			  topK_identifier.add(target.getNormolizedLowerBound() + "&&" + target.getId());
			  Collections.sort(topK_identifier, new MyComparator());
		  }else{
			  topK_identifier.add(target.getNormolizedLowerBound() + "&&" + target.getId());
			  Collections.sort(topK_identifier, new MyComparator());
			  topK_identifier.remove(topK_identifier.size() - 1);
		  }
	  }
	  
	  static enum MoreIterations {  
		  numberOfIterations  
	  }
	  

	@Override
	public int run(String[] args) throws Exception {
		
		long startTime=System.currentTimeMillis();
		
		int iterationCount = 0; // counter to set the ordinal number of the  
        						// intermediate outputs  
		Job job1;  
		long terminationValue = 1;  
		
		while (terminationValue > 0){ 
			job1 = getJobConf(args); // get the job configuration  
		    String input, output;  
		  
		      // setting the input file and output file for each iteration  
		      // during the first time the user-specified file will be the input whereas  
		      // for the subsequent iterations  
		      // the output of the previous iteration will be the input  
		    if (iterationCount == 0) {   
		      // for the first iteration the input will be the first input argument  
		      input = args[0];  
		    } else {  
		      // for the remaining iterations, the input will be the output of the  
		      // previous iteration  
		      input = args[1] + iterationCount;  
		    }  
		    
		    output = args[1] + (iterationCount + 1); // setting the output file  
		  
		    FileInputFormat.setInputPaths(job1, new Path(input));   
		    FileOutputFormat.setOutputPath(job1, new Path(output));   
		  
		    job1.waitForCompletion(true);   
		  
		    Counters jobCntrs = job1.getCounters();  
		    terminationValue = jobCntrs.findCounter(MoreIterations.numberOfIterations).getValue();  
		    iterationCount ++;	    
		}

		long endTime=System.currentTimeMillis(); //获取结束时间  
		System.out.println("Run Time: "+(endTime-startTime)+"ms");
		
		return 0;
	}

	@Override
	protected Job getJobConf(String[] args) throws Exception {
		
		JobInfo jobInfo = new JobInfo(){

			@Override
			public Class<?> getJarByClass() {
				return QueryDriver.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				return QueryTraverseMapper.class;
			}

			@Override
			public Class<? extends Reducer> getCombinerClass() {
				return null;
			}

			@Override
			public Class<? extends Reducer> getReducerClass() {
				return QueryTraverseReducer.class;
			}

			@Override
			public Class<?> getOutputKeyClass() {
				return Text.class;
			}

			@Override
			public Class<?> getOutputValueClass() {
				return Text.class;
			}

			@Override
			public Class<? extends InputFormat> getInputFormatClass() {
				return TextInputFormat.class;
			}

			@Override
			public Class<? extends OutputFormat> getOutputFormatClass() {
				return TextOutputFormat.class;
			}
		};
			
		return setupJob("query", jobInfo);
	}
	
}
