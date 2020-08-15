import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopHashtagsBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	
	private long startingTimeTop;
	
	private Map<String, DataEntity> topHashtags = null;

	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		topHashtags = new ConcurrentHashMap<String, DataEntity>();
		startingTimeTop = System.currentTimeMillis();
	}
	
	public void execute(Tuple tuple) {
		
		String currentHashtag = tuple.getStringByField("hashtag");
		DataEntity currentEntity = (DataEntity) tuple.getValueByField("entity");
		
		if(!topHashtags.containsKey(currentHashtag)) {
			topHashtags.put(currentHashtag, currentEntity);	
		}/**else {
			if(topHashtags.get(currentHashtag).frequency < currentEntity.frequency) {
				topHashtags.put(currentHashtag, currentEntity);
			}
		}**/
		
		long presentTimeTop = System.currentTimeMillis();
		
		if((System.currentTimeMillis() - startingTimeTop) >= 10000 && !topHashtags.isEmpty()) {
			emitSortedTop();
			startingTimeTop = System.currentTimeMillis();
		}
		
	}
	
	
	
	public void emitSortedTop() {
		
		HashMap<String, DataEntity> sortedTopHashtags = sortTopMap(topHashtags);
		ArrayList<String> hashtags = new ArrayList<String>(); 
		long timeStamp = System.currentTimeMillis();
		
		//FileWriter fileWriter;
		for(Map.Entry<String, DataEntity> eachEntry: sortedTopHashtags.entrySet()) {
			
			/**try {
				fileWriter = new FileWriter("/s/chopin/k/grad/nazia4/PA2/output/Top.txt", true);
				BufferedWriter bw = new BufferedWriter(fileWriter);
				bw.write(eachEntry.getKey() + " " + eachEntry.getValue().frequency);
				bw.write("\n");
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}**/
			
			hashtags.add(eachEntry.getKey());
			if(hashtags.size() >= 100) {
				break;
			}
		}
		this.collector.emit(new Values(timeStamp, hashtags));		
	}
	
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "hashtags"));
	}
	
	
	public static HashMap<String, DataEntity> sortTopMap(Map<String, DataEntity> unsortedMap){

		List<DataEntity> sortList = new LinkedList<DataEntity>(unsortedMap.values());
		if(sortList.size() >= 2) {
			Collections.sort(sortList);
		}
		
		HashMap<String, DataEntity> sortedMap = new LinkedHashMap<String, DataEntity>();
		for(DataEntity entry: sortList) {
			sortedMap.put(entry.hashtag, entry);
		}
		
		return sortedMap;
	}

}

