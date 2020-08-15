import java.io.Serializable;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class NonParallelTopology implements Serializable{
	
	private static final String TWEET_SPOUT_ID = "tweet-spout";
	private static final String HASHTAG_BOLT_ID = "hashtag-bolt";
	private static final String LOSSY_BOLT_ID = "lossy-bolt";
	private static final String TOP_BOLT_ID = "top-bolt";
	private static final String PRINT_BOLT_ID = "print-bolt";
	private static final String TOPOLOGY_NAME = "nonparallel-topology";
	
	private static final String consumerKey = "4ZwPnO0oRcccCtNdmymlQ5XV0";
	private static final String consumerSecret = "hPvn2dUDz8ewwEtDXdiVvVB4hzflZhg7H6PViy3OTXaCto66HR"; 
	private static final String accessToken = "1231428737322717185-sL23GpgQM8ShIlGXMUtxUCHYatlwyc"; 
	private static final String accessTokenSecret = "sQVY3J5qks7ph9kLmz1Bc9storBTFGENcx3lqYH3Rf1mz";
	
	public static void main(String[] args) throws Exception {
		
		TweetStreamSpout spout = new TweetStreamSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret);
		HashtagEmitBolt hashtagBolt = new HashtagEmitBolt();
		LossyCountingBolt lossyBolt;
		if(args.length > 1) {
			lossyBolt = new LossyCountingBolt(Double.parseDouble(args[1]));
		}else {
			lossyBolt = new LossyCountingBolt();
		}
		TopHashtagsBolt topBolt = new TopHashtagsBolt();
		HashtagsPrintBolt printBolt;
		if(args.length > 0) {
			printBolt = new HashtagsPrintBolt(args[0]);
		}else {
			printBolt = new HashtagsPrintBolt();
		}
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(TWEET_SPOUT_ID, spout);
		
		builder.setBolt(HASHTAG_BOLT_ID, hashtagBolt).shuffleGrouping(TWEET_SPOUT_ID);
		builder.setBolt(LOSSY_BOLT_ID, lossyBolt).fieldsGrouping(HASHTAG_BOLT_ID, new Fields("hashtag"));
		builder.setBolt(TOP_BOLT_ID, topBolt).globalGrouping(LOSSY_BOLT_ID);
		builder.setBolt(PRINT_BOLT_ID, printBolt).globalGrouping(TOP_BOLT_ID);
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		//Thread.sleep(10000);
		//cluster.killTopology(TOPOLOGY_NAME);
		//cluster.shutdown();
	}

}

