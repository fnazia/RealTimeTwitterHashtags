import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;

public class HashtagsPrintBolt extends BaseRichBolt{
	
	private FileWriter fileWriter;
	private BufferedWriter bufferedWriter;
	SimpleDateFormat timeFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	long timeStart;
	private String filepath;
	
	public HashtagsPrintBolt() {
		this.filepath = null;
	}
	
	public HashtagsPrintBolt(String p) {
		this.filepath = p;
	}
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		if(filepath != null) {
			try {
				//fileWriter = new FileWriter("/Users/naziafarhat/eclipse-workspace/twitter2/output/hashtags.txt", true);
				fileWriter = new FileWriter(filepath, true);
				bufferedWriter = new BufferedWriter(fileWriter);
			}catch(Exception e) {
				//System.out.println("File could not be written");
				e.printStackTrace();
			}
		}
		timeStart = System.currentTimeMillis();
	}
	
	public void execute(Tuple tuple) {
		long timeNow = System.currentTimeMillis(); //tuple.getLongByField("timestamp");
		ArrayList<String> hashtags = (ArrayList<String>) tuple.getValueByField("hashtags");
		if((timeNow - timeStart) >= 10000 && !hashtags.isEmpty()) {
			Date timeStamp = new Date();
			if(filepath != null) {
				try {
					bufferedWriter.write(timeFormat.format(timeStamp) + "\t" + hashtags + "\n");
					bufferedWriter.flush();	
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println(timeFormat.format(timeStamp) + " " + hashtags);
			timeStart = System.currentTimeMillis();
		}
	}
	
	@Override
	public void cleanup() {
		try {
			bufferedWriter.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}

