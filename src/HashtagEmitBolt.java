import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
import java.util.Map;

public class HashtagEmitBolt extends BaseRichBolt{
	
private OutputCollector collector;
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void execute(Tuple tuple) {
		Status tweet = (Status) tuple.getValueByField("tweet");
		for(HashtagEntity hashtag: tweet.getHashtagEntities()) {
			if(!hashtag.getText().isEmpty()) {
				if(hashtag.getText().matches("[A-Za-z0-9]+\\.?\\_?\\-?\\,?\\;?\\@?")) {
					/**FileWriter fileWriter;
					try {
						fileWriter = new FileWriter("/s/chopin/k/grad/nazia4/PA2/output/Hashtag.txt", true);
						BufferedWriter bw = new BufferedWriter(fileWriter);
						bw.write(hashtag.getText().toLowerCase());
						bw.write("\n");
						bw.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}**/
					
					this.collector.emit(new Values(hashtag.getText().toLowerCase()));
				}
				
			}
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag"));
	}

}
