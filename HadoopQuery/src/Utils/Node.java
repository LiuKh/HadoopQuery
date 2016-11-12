package Utils;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score|upper_bound|lower_bound|n_upper_bound|n_lower_bound|topK_score
//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score
public class Node {
	
	public static enum Color{
		WHITE,
		GRAY,
		BLACK,
		RED
	}
	
	private String id;
	private int distance;
	private double rankingScore;
	private double pRankingScore;
	private List<String> edges = new ArrayList<String>();
	private Color color = Color.WHITE;
	private double upperBound;
	private double lowerBound;
	private double normolizedUpperBound;
	private double normolizedLowerBound;
	private double KthLargestScore;
	
	private String parent;
	
	public Node(){
		rankingScore = 0;
		distance = Integer.MAX_VALUE;
		color = Color.WHITE;
		parent = null;
		pRankingScore = 0;
		this.upperBound = 0;
		this.lowerBound = 0;
		this.normolizedLowerBound = -1;
		this.normolizedUpperBound = -1;
		this.KthLargestScore = -1;
	}
	
	public Node(String nodeInfo){
		String[] inputVal = nodeInfo.split("\t");
		String key = "";
		String val = "";
		
		try{
			key = inputVal[0];
			val = inputVal[1];
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
		
		this.id = key;
		
		String[] tokens = val.split("\\|");
		//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score|upper_bound|lower_bound|n_upper_bound|n_lower_bound
		//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score
		for(String s : tokens[0].split(",")){
			if(s.length() > 0){
				edges.add(s);
			}
		}
		
		if(tokens[1].equals("Integer.MAX_VALUE")){
			this.distance = Integer.MAX_VALUE;
		}else{
			this.distance = Integer.parseInt(tokens[1]);
		}
		
		this.rankingScore = Double.parseDouble(tokens[2]);
		this.color = Color.valueOf(tokens[3]);
		this.parent = tokens[4];
		this.pRankingScore = Double.parseDouble(tokens[5]);
		this.upperBound = Double.parseDouble(tokens[6]);
		this.lowerBound = Double.parseDouble(tokens[7]);
		this.normolizedUpperBound = Double.parseDouble(tokens[8]);
		this.normolizedLowerBound = Double.parseDouble(tokens[9]);
		this.KthLargestScore = Double.parseDouble(tokens[10]);
	}
	
	//source<tab>adjacency_list|distance_from_the_source|ranking score|color|parentNode|parent_ranking_score
	public Text getNodeInfo(){
		StringBuilder sb = new StringBuilder();
		for(String v : edges){
			sb.append(v).append(",");
		}
		
		sb.append("|");
		
		if(this.distance < Integer.MAX_VALUE){  
			sb.append(this.distance);  
		} else {  
			sb.append("Integer.MAX_VALUE");  
		}
		
		sb.append("|");
		
		sb.append(String.valueOf(getRankingScore()));
		
		sb.append("|");
		
		sb.append(getColor()).append("|");
		
		sb.append(getParent());
		
		sb.append("|");
		
		sb.append(getpRankingScore());	
		
		sb.append("|");
		
		sb.append(getUpperBound());
		
		sb.append("|");
		
		sb.append(getLowerBound());
		
		sb.append("|");
		
		sb.append(getNormolizedUpperBound());
		
		sb.append("|");
		
		sb.append(getNormolizedLowerBound());
		
		sb.append("|");
		
		sb.append(getKthLargestScore());
		
		return new Text(sb.toString());
	}
	
	//getters and setters
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public double getRankingScore() {
		return rankingScore;
	}

	public void setRankingScore(double rankingScore) {
		this.rankingScore = rankingScore;
	}

	public List<String> getEdges() {
		return edges;
	}

	public void setEdges(List<String> edges) {
		this.edges = edges;
	}

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	public double getpRankingScore() {
		return pRankingScore;
	}

	public void setpRankingScore(double pRankingScore) {
		this.pRankingScore = pRankingScore;
	}

	public double getUpperBound() {
		return upperBound;
	}

	public void setUpperBound(double upperBound) {
		this.upperBound = upperBound;
	}

	public double getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(double lowerBound) {
		this.lowerBound = lowerBound;
	}

	public double getNormolizedUpperBound() {
		return normolizedUpperBound;
	}

	public void setNormolizedUpperBound(double normolizedUpperBound) {
		this.normolizedUpperBound = normolizedUpperBound;
	}

	public double getNormolizedLowerBound() {
		return normolizedLowerBound;
	}

	public void setNormolizedLowerBound(double normolizedLowerBound) {
		this.normolizedLowerBound = normolizedLowerBound;
	}

	public double getKthLargestScore() {
		return KthLargestScore;
	}

	public void setKthLargestScore(double kthLargestScore) {
		KthLargestScore = kthLargestScore;
	}
	
	
}
