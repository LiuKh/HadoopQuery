import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


public class Query {

	public static void main(String[] args) throws Exception { 
		
		String[] paths = new String[2];
		paths[0] = HDFS + "/user/root/input";
		paths[1] = HDFS + "/user/root/output";
		
	    int res = ToolRunner.run(new Configuration(), new QueryDriver(), paths);
	    
//	    if(args.length != 2){  
//	      System.err.println("Usage: <in> <output name> ");  
//	    }  
	    
	    System.exit(res);  
	} 

	public static final String HDFS = "hdfs://192.168.44.200:9000";
}
