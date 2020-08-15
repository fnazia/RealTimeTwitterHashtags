import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TweetStreamSpout extends BaseRichSpout{
	
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream twitterStream;
    
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    
    public TweetStreamSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
    	this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
    }
    
    //@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;
        StatusListener listener = new StatusListener() {
            //@Override
            public void onStatus(Status status) {
            	
            	//FileWriter fileWriter;
            	
            	try {
            		
            		//fileWriter = new FileWriter("/s/chopin/k/grad/nazia4/PA2/output/Spout.txt", true);
			//BufferedWriter bw = new BufferedWriter(fileWriter);
					
					/**if(status.isRetweet()){ 
					     twitterStatus.setText(status.getRetweetedStatus().getText()); **/
					     
			if (status.getRetweetedStatus() != null) {
				queue.put(status.getRetweetedStatus());
				//bw.write(status.getRetweetedStatus().getText());
				//bw.write("\n");

				//bw.flush();
			}else {
				queue.put(status);
				//bw.write(status.getText());
				//bw.write("\n");

				//bw.flush();
			}
					
					/**queue.put(status);
					bw.write(status.getText());
					bw.write("\n");

					bw.flush();**/
					
					/**String language = status.getUser().getLang();
					if(!language.isEmpty()) {
						queue.put(status);
						bw.write(status.getText());
						bw.write("\n");

						bw.flush();
					}**/
            	} catch (Exception e) {
            		e.printStackTrace();
            	}
                
            }
            //@Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }
            //@Override
            public void onTrackLimitationNotice(int i) {
            }
            //@Override
            public void onScrubGeo(long l, long l1) {
            }
            //@Override
            public void onException(Exception e) {
            }
	    public void onStallWarning(StallWarning warning) {
		// TODO Auto-generated method stub
	    }
            
        };
        
        ConfigurationBuilder cb = new ConfigurationBuilder();
		
        cb.setDebugEnabled(true)
           .setJSONStoreEnabled(true)
           .setTweetModeExtended(true)
           .setOAuthConsumerKey(consumerKey)
           .setOAuthConsumerSecret(consumerSecret)
           .setOAuthAccessToken(accessToken)
           .setOAuthAccessTokenSecret(accessTokenSecret);
        
        TwitterStreamFactory fact = new TwitterStreamFactory(cb.build());
        twitterStream = fact.getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
        
        
        /**if(keyWords.length == 0) {
        	this.twitterStream.sample();
        }else {
        	FilterQuery keyW = new FilterQuery().track(keyWords);
            this.twitterStream.filter(keyW);
        }**/
        
        
        
    }
    
    
    //@Override
    public void nextTuple() {
        Status ret = this.queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
        	this.collector.emit(new Values(ret));
        	if(ret.getLang().toLowerCase() == "en") {
        		this.collector.emit(new Values(ret));
        	}
        }
    }
    @Override
    public void close() {
        twitterStream.shutdown();
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    
    @Override
    public void ack(Object id) {
    }
    @Override
    public void fail(Object id) {
    }
    //@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}

