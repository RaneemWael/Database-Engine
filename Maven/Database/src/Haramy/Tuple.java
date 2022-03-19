package Haramy;

import java.util.Date;
import java.util.Hashtable;
import java.util.Set;

@SuppressWarnings("serial")
public class Tuple implements java.io.Serializable {

	Hashtable<String, Object> htblColNameData;
	
	public Tuple(Hashtable<String, Object> htblColNameData) {
		this.htblColNameData = htblColNameData;
		long millis = System.currentTimeMillis(); 
		Date TouchDate = new Date(millis);
		htblColNameData.put("TouchDate", TouchDate);
	}
	
	//getters and setters
	public Hashtable<String, Object> getHtblColNameData() {
		return htblColNameData;
	}
	//end of setters and getters
	
	public Object getClusterKey(String clusterKey) {
		
		Set <String> keys = htblColNameData.keySet();
		
		Object keyValue = null;
		
		for (String key: keys) {
			if (key.equals(clusterKey)) {
				keyValue = htblColNameData.get(key);
			}
		}
		return keyValue;
	}
	
	public Object getColKey(String colName) {
		
		Set <String> keys = htblColNameData.keySet();
		
		Object keyValue = null;
		
		for (String key: keys) {
			if (key.equals(colName)) {
				keyValue = htblColNameData.get(key);
			}
		}
		return keyValue;
	}
	
	public void viewTuple() {
		
		Set <String> keys = htblColNameData.keySet();
		
		Object keyValue = null;
		
		for (String key: keys) {
			keyValue = htblColNameData.get(key);
			System.out.print(keyValue + ", ");
		}
		
	}
	
	public String toString() {
		
		Set <String> keys = htblColNameData.keySet();
		
		Object keyValue = null;
		String res = "";
		
		for (String key: keys) {
			keyValue = htblColNameData.get(key);
			res = res + keyValue + ", ";
		}
		
		return res;
		
	}

}
