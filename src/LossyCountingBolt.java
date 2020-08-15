import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class LossyCountingBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	
	private double epsilon = 0.005;
	private double threshold;
	private int bucket_current = 1;
	private int bucket_width = (int)Math.ceil(1/epsilon);
	private int numberOfEntries = 0;
	private long startingTime;
	
	private Map<String, DataEntity> hashtagStorage = null;

	public LossyCountingBolt() {
		this.threshold = 0.1;
	}
	
	public LossyCountingBolt(double s) {
		this.threshold = s;
	}
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.startingTime = System.currentTimeMillis();
		hashtagStorage = new ConcurrentHashMap<String, DataEntity>(); 
	}
	
	public void execute(Tuple tuple) {
		
		String currentHashtag = tuple.getStringByField("hashtag");
		if(hashtagStorage.containsKey(currentHashtag)) {
			DataEntity hashtagExist = hashtagStorage.get(currentHashtag);
			hashtagExist.frequency += 1;
			hashtagStorage.put(currentHashtag, hashtagExist);
		}else {
			DataEntity hashtagEntry = new DataEntity();
			hashtagEntry.hashtag = currentHashtag;
			hashtagEntry.frequency = 1;
			hashtagEntry.error = bucket_current - 1;
			hashtagStorage.put(currentHashtag, hashtagEntry);
		}
		
		numberOfEntries++;
		
		if((numberOfEntries%bucket_width) == 0) {
			if(!hashtagStorage.isEmpty()) {
				deleteEntries();
			}
			bucket_current += 1;
		}
		
		long presentTime = System.currentTimeMillis();
		//System.out.println(hashtagStorage);
		if((presentTime - this.startingTime) >= 10000 && !hashtagStorage.isEmpty()) {
			//System.out.println(hashtagStorage);
			emitHashtags();
			this.startingTime = presentTime;
		}	
	}
	
	public void deleteEntries() {
		for(String hashKey: hashtagStorage.keySet()) {
			DataEntity d = hashtagStorage.get(hashKey);
			if((d.frequency + d.error) <= bucket_current) {
				hashtagStorage.remove(hashKey);
			}/**else {
				if(d.frequency < ((threshold - epsilon)*numberOfEntries)) {
					hashtagStorage.remove(hashKey);
				}
			}**/
		}
		if(threshold != 0 && !hashtagStorage.isEmpty()) {
			for(String hashKey: hashtagStorage.keySet()) {
				DataEntity d = hashtagStorage.get(hashKey);
				if(d.frequency < ((threshold - epsilon)*hashtagStorage.size())) {
					hashtagStorage.remove(hashKey);
				}
			}
		}
	}
	
	public void emitHashtags() {
		
		int i = 0;
		HashMap<String, DataEntity> sortedStorage = sortMap(hashtagStorage);
		
		for(Map.Entry<String, DataEntity> eachEntry: sortedStorage.entrySet()) {
			/**FileWriter fileWriter;
			//hashtags.add(eachEntry.getValue());
			
			try {
				fileWriter = new FileWriter("/s/chopin/k/grad/nazia4/PA2/output/Lossy.txt", true);
				BufferedWriter bw = new BufferedWriter(fileWriter);
				bw.write(eachEntry.getKey() + " " + eachEntry.getValue().frequency);
				bw.write("\n");
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}**/
			
			this.collector.emit(new Values(eachEntry.getKey(), eachEntry.getValue()));	
			i += 1;
			if(i >= 100) {
				break;
			}
		}	
	}
	
	
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("timestamp", "hashtags"));
		declarer.declare(new Fields("hashtag", "entity"));
	}
	
	public static HashMap<String, DataEntity> sortMap(Map<String, DataEntity> unsortedMap){

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

