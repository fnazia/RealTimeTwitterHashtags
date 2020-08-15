import java.io.Serializable;

public class DataEntity implements Serializable, Comparable<DataEntity> {
	
	public String hashtag;
	public int frequency;
	public int error;
	
	public int getFreq(String ht) {
		if(hashtag.equals(ht)) {
			return this.frequency;
		}else {
			return 0;
		}
	}

	public int compareTo(DataEntity o) {
		// TODO Auto-generated method stub
		return ((DataEntity)o).frequency - this.frequency;
	}

}
