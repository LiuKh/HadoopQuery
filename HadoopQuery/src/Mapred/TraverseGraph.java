package Mapred;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Node;
import Utils.Node.Color;


public class TraverseGraph {

	
	public static class TraverseMapper extends Mapper<Object, Text, Text, Text>{
		protected void map(Object key, Text value, Context context, Node inNode) throws IOException, InterruptedException{
			if(inNode.getColor() == Color.GRAY){
				//compute UpperBound and LowerBound
				inNode.setNormolizedUpperBound(computeNormalizedBound(inNode.getUpperBound(), inNode.getDistance()));
				inNode.setNormolizedLowerBound(computeNormalizedBound(inNode.getLowerBound(), inNode.getDistance()));
				
				//test if this path is not qualified
				if(inNode.getNormolizedUpperBound() < inNode.getKthLargestScore()){
					return;
				}else{
				
					if(inNode.getId().contains("vulnerability")){
						inNode.setColor(Node.Color.RED);
					}else{
						
						
						//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score|upper_bound|lower_bound
						//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score
						for(String neighbor : inNode.getEdges()){
							
							//make sure that the query will not go back
							if(getId(inNode.getParent()).equals(getId(neighbor))){
								continue;
							}
							
							Node adjacentNode = new Node();
							
							adjacentNode.setId(getId(neighbor));
							adjacentNode.setDistance(inNode.getDistance() + 1);
							adjacentNode.setColor(Node.Color.GRAY);
							adjacentNode.setUpperBound(0);
							adjacentNode.setLowerBound(0);
							
							String relation = getRelation(neighbor);						
							adjacentNode.setParent(relation + "::" + inNode.getId());
							adjacentNode.setpRankingScore(inNode.getLowerBound());
							
							context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
						}
						
						inNode.setColor(Node.Color.BLACK);
					}
				}
			}
			
			context.write(new Text(inNode.getId()), inNode.getNodeInfo());
		}
		
		private static String getId(String s){
			if(s.equals("source")){
				return "source";
			}else{
				return s.split("::")[1];
			}
		}
		
		private static String getRelation(String s){
			String relation = s.split("::")[0];
			
			if(relation.contains("s")){
				return "f";
			}else if(relation.contains("f")){
				return "s";
			}else{
				return relation;
			}
		}
		
		
		private static double computeNormalizedBound(double bound, int distance){
			return bound / (Math.pow(2, distance - 1) + (-0.5));
		}

	}
	
	
	public static class TraverseReducer extends Reducer<Text, Text, Text, Text> {  
        protected Node reduce(Text key, Iterable<Text> values, Context context,  
                Node outNode) throws IOException, InterruptedException {  
            // set the node id as the key  
            outNode.setId(key.toString()); 
            //source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score|upper_bound|lower_bound
            //source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score
            for (Text value : values) {  
                Node inNode = new Node(key.toString() + "\t" + value.toString()); 
                
                if(inNode.getKthLargestScore() != -1)
                	outNode.setKthLargestScore(inNode.getKthLargestScore());
                
                if(outNode.getEdges().size() == 0 && inNode.getEdges().size() > 0){
                	outNode.setEdges(inNode.getEdges());
                }
                
                if(computeEdgeScore(inNode.getParent(), inNode.getpRankingScore()) > outNode.getLowerBound()){
                	outNode.setDistance(inNode.getDistance());
	                outNode.setParent(inNode.getParent());
	                outNode.setRankingScore(computeEdgeScore(inNode.getParent(), inNode.getpRankingScore()));
	                outNode.setpRankingScore(inNode.getpRankingScore());
	                
	                if(outNode.getLowerBound() == 0){//the lower bound hasn't set, its lower bound is defined by the parent node
	                	outNode.setLowerBound(computeEdgeScore(inNode.getParent(), inNode.getpRankingScore())); // if BFS got the node already, the score will not change
	                }else{
	                	double max = Math.max(inNode.getLowerBound(), outNode.getLowerBound());
	                	outNode.setLowerBound(max);
	                }
	                
	                if(outNode.getLowerBound() != 0){
	                	outNode.setUpperBound(outNode.getLowerBound());
	                }else{
	                	outNode.setUpperBound(0);
	                }
                	
	                if(inNode.getNormolizedUpperBound() != -1){
	                	outNode.setNormolizedUpperBound(inNode.getNormolizedUpperBound());
	                }
	                
	                if(inNode.getNormolizedLowerBound() != -1){
	                	outNode.setNormolizedLowerBound(inNode.getNormolizedLowerBound());
	                }
	                
	                if(outNode.getColor().ordinal() < inNode.getColor().ordinal())
	                	outNode.setColor(inNode.getColor());
                }
            }  
            
            if(outNode.getColor() == Node.Color.RED){
            	if(outNode.getNormolizedUpperBound() < outNode.getKthLargestScore())
            		return outNode;
            }
            
            context.write(key, new Text(outNode.getNodeInfo()));  
  
            return outNode;  
        }  
        
        private static double computeEdgeScore(String parent, double pRankingScore){
        	if(parent.equals("source")){
        		return 1.0;
        	}
        	
    		if(parent.contains("s")){
    			double current_score = pRankingScore * (1 / Math.pow(2, -1));//Beta=2
    			return current_score;
    		}else if(parent.contains("f")){
    			double current_score = pRankingScore * (1 / Math.pow(2, +1));//Beta=2
    			return current_score;
    		}else{
    			return pRankingScore;
    		}
    	}
	}

}
