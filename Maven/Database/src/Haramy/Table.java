package Haramy;

import java.util.Hashtable;
import java.util.Set;


@SuppressWarnings("serial")
public class Table implements java.io.Serializable {
	
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String, String> htblColNameType;
	Hashtable<String, String> htblColBIndex;
	Hashtable<String, String> htblColRIndex;
	int pageCount;
	
	public Table (String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		htblColBIndex = new Hashtable<String, String>();
		htblColRIndex = new Hashtable<String, String>();
		pageCount = 0;
	}


	//getters and setters
	public String getStrTableName() {
		return strTableName;
	}

	public Hashtable<String, String> getHtblColBIndex() {
		return htblColBIndex;
	}


	public Hashtable<String, String> getHtblColRIndex() {
		return htblColRIndex;
	}


	public void setStrTableName(String strTableName) {
		this.strTableName = strTableName;
	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public void setStrClusteringKeyColumn(String strClusteringKeyColumn) {
		this.strClusteringKeyColumn = strClusteringKeyColumn;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public int getPageCount() {
		return pageCount;
	}
	
	public void setPageCount(int pagesNumber) {
		this.pageCount = pagesNumber;
	}
	//end of setters and getters
	
	public String getClusterType() {
		
		Set <String> keys = htblColNameType.keySet();
		
		for (String key: keys) {
			if (key.equals(strClusteringKeyColumn)) {
				switch(htblColNameType.get(key)) {
					case "java.lang.Integer": return "Integer";
					case "java.lang.String": return "String";
					case "java.lang.Double": return "Double";
					case "java.lang.Boolean": return "Boolean";
					case "java.util.Date": return "Date";
					default: return "Polygon";
				}
			}
		}
		return "";
	}
	
	public String getColType(String colName) {
		
		Set <String> keys = htblColNameType.keySet();
		
		for (String key: keys) {
			if (key.equals(colName)) {
				switch(htblColNameType.get(key)) {
					case "java.lang.Integer": return "Integer";
					case "java.lang.String": return "String";
					case "java.lang.Double": return "Double";
					case "java.lang.Boolean": return "Boolean";
					case "java.util.Date": return "Date";
					default: return "Polygon";
				}
			}
		}
		return "";
	}
	
	public void viewTable () {
		
		if (this.pageCount == 0) {
			System.out.println("empty");
		}
		
		for (int i=1; i<=this.pageCount; i++) {
			System.out.println("PAGE: " + i);
			System.out.println();
			String filename = this.strTableName + i;
			String path = "data/" + filename;
			Page p = (Page) DBApp.deserialize(path);
			p.viewPage(this.strClusteringKeyColumn);
			p = null;
			System.out.println("\n");
		}
	}
	
	public String indexExists (String colName, String colType) {
	
		if (colType.equals("Polygon")) {
			
			Set <String> keys = htblColRIndex.keySet();
			
			for (String key: keys) {
				if (key.equals(colName))
					return htblColRIndex.get(key);
			}
		}
		
		else {
			
			Set <String> keys = htblColBIndex.keySet();
			
			for (String key: keys) {
				if (key.equals(colName))
					return htblColBIndex.get(key);
			}
		}
		
		return null;
		
	}
	
}
