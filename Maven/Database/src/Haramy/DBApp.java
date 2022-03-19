package Haramy;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;


public class DBApp {

	public void init( ) {}
	
	public void serialize (String path, Object o) {
		try {
			FileOutputStream file = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(file);
			out.writeObject(o);
			out.close();
			file.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Object deserialize (String path) {
		Object o = null;
		try {
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(file);
			o = in.readObject();
			in.close();
			file.close();
			return o;
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return o;
	}
	
	public String getType (String type) {

		switch(type) {
			case "java.lang.Integer": return "Integer";
			case "java.lang.String": return "String";
			case "java.lang.Double": return "Double";
			case "java.lang.Boolean": return "Boolean";				
			case "java.util.Date": return "Date";
			case "java.lang.integer": return "Integer";
			case "java.lang.string": return "String";
			case "java.lang.double": return "Double";
			case "java.lang.boolean": return "Boolean";				
			case "java.util.date": return "Date";
			default: return "Polygon";
		}
	}
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] info = line.split(",");
			
			for (int i=0; i<info.length; i++) {
				if (info[0].equals(strTableName)) {
					br.close();
					throw new DBAppException("Table name already exists!");
				}
			}
			line = br.readLine();
		}
		br.close();
		
		Set <String> keys = htblColNameType.keySet();
		for (String key: keys) {
				if (!((htblColNameType.get(key).equalsIgnoreCase("java.lang.Integer")) || (htblColNameType.get(key).equalsIgnoreCase("java.lang.String")) || (htblColNameType.get(key).equalsIgnoreCase("java.lang.Double")) || (htblColNameType.get(key).equalsIgnoreCase("java.lang.Boolean")) || (htblColNameType.get(key).equalsIgnoreCase("java.util.Date")) || (htblColNameType.get(key).equalsIgnoreCase("java.awt.Polygon"))))
					throw new DBAppException("Column type not supported!");
		}
		
		Table t = new Table (strTableName, strClusteringKeyColumn, htblColNameType);
		
		String tablePath = "data/" + strTableName;
		serialize(tablePath, t);
		
		t = null;
			
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter("data/metadata.csv", true));
			
			//loop on hashtable
			Set <String> keys1 = htblColNameType.keySet();
			for (String key: keys1) {
				String info;
				if (key.equals(strClusteringKeyColumn))
					info = strTableName + "," + key + "," + htblColNameType.get(key) + "," + "True," + "False";
				else
					info = strTableName + "," + key + "," + htblColNameType.get(key) + "," + "False," + "False";
				writer.write(info);
				writer.newLine();
			}
			writer.write(strTableName + ",TouchDate,java.util.Date,False,False");
			writer.newLine();
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void checkTableName (String strTableName) throws IOException, DBAppException {
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] info = line.split(",");
			
			for (int i=0; i<info.length; i++) {
				if (info[0].equals(strTableName)) {
					br.close();
					return;
				}
			}
			line = br.readLine();
		}
		br.close();
		
		throw new DBAppException("Table name doesn't exist!");
		
	}
	
	public boolean checkDataTypes (String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException {
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] info = line.split(",");
			
			for (int i=0; i<info.length; i++) {
				
				if (info[0].equals(strTableName)) {
					String colName = info[1];
					Object colValue = null;
					
					Set <String> keys = htblColNameValue.keySet();
					for (String key: keys) {
						if (key.equals(colName)) {
							colValue = htblColNameValue.get(key);
					
							if (getType(info[2]).equals("String")) {
								if (!(colValue instanceof String)) {
									System.out.println("Please enter " + colName + " in the format of " + info[2] + "!");
									br.close();
									return false;
								}
							}
							else if (getType(info[2]).equals("Integer")) {
								if (!(colValue instanceof Integer)) {
									System.out.println("Please enter " + colName + " in the format of " + info[2] + "!");
									br.close();
									return false;
								}
							}
							else if (getType(info[2]).equals("Date")) {
								if (!(colValue instanceof Date)) {
									System.out.println("Please enter " + colName + " in the format of " + info[2] + "!");
									br.close();
									return false;
								}
							}
							else if (getType(info[2]).equals("Boolean")) {
								if (!(colValue instanceof Boolean)) {
									System.out.println("Please enter " + colName + " in the format of " + info[2] + "!");
									br.close();
									return false;
								}
							}
							else if (getType(info[2]).equals("Double")) {
								if (!(colValue instanceof Double)) {
									System.out.println("Please enter " + colName + " in the format of " + info[2] + "!");
									br.close();
									return false;
								}
							}
							else if (getType(info[2]).equals("Polygon")) {
								if (!(colValue instanceof Polygon)) {
									System.out.println("Please enter " + colName + " in the format of " + info[2] + "!");
									br.close();
									return false;
								}
							}
						}
					}
				}
			}
			line = br.readLine();
		}
		br.close();
		return true;
	}
	
	public void checkAllCols (String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException, DBAppException {
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] info = line.split(",");
				
			if (info[0].equals(strTableName) && !(info[1].equals("TouchDate"))) {
				String colName = info[1];
				boolean present = false;
				Set <String> keys = htblColNameValue.keySet();
				for (String key: keys) {
					present = false;
					if (key.equals(colName)) { 
						present = true;
						break;
					}
				}
				if (present == false) {
					br.close();
					throw new DBAppException ("Please include values for all columns!");
				}
			}
			line = br.readLine();
		}
		br.close();
	}
	
	public void checkColExists (String strTableName, String strColName) throws IOException, DBAppException {
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		String line = br.readLine();
		
		boolean present = false;
		while (line != null) {
			String[] info = line.split(",");
				
			if (info[0].equals(strTableName) && !(info[1].equals("TouchDate"))) {
				String colName = info[1];
				if (strColName.equals(colName)) { 
					present = true;
					br.close();
					return;
				}
			}
			line = br.readLine();
		}
		
		if (present == false) {
			br.close();
			throw new DBAppException ("Please enter a valid column!");
		}
		br.close();
		
	}
	
	public int getPageMax() throws IOException {
		Properties prop = new Properties();
		FileInputStream ip = new FileInputStream("config/DBApp.properties");
		prop.load(ip);
		int max = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		return max;
	}
	
	public int getNodeMax() throws IOException {
		Properties prop = new Properties();
		FileInputStream ip = new FileInputStream("config/DBApp.properties");
		prop.load(ip);
		int max = Integer.parseInt(prop.getProperty("NodeSize"));
		return max;
	}
	
	public int getPageLast(Table t, Object clusterKey) throws IOException {
		
		int i = 1;
		
		while (i<=t.getPageCount()) {
			String filename = t.getStrTableName() + i;
			String path = "data/" + filename;
			
			Page p = (Page) deserialize(path);
			Vector<Tuple> rows = p.getRows();
				
			if (t.getClusterType().equals("Integer")) {
				int high = (int) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if ((int)clusterKey >= high) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("Double")) {
				double high = (double) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if ((double)clusterKey >= high) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("String")) {
				String high = (String) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high.compareTo((String)clusterKey) <= 0) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("Date")) {
				Date high = (Date) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high.compareTo((Date)clusterKey) <= 0) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("Polygon")) {
				PolygonC high = new PolygonC ((Polygon)rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn()));
				if (high.compareTo(((Polygon) clusterKey)) <= 0) {
					p = null;
					i++;
				}
				else {
					//System.out.println(high.compareTo(((Polygon)clusterKey)));
					return i;
				}
			}
			else if (t.getClusterType().equals("Boolean")) {
				boolean high = (boolean) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high == false) {
					if ((boolean)clusterKey == true) {
						p = null;
						i++;
					}
					else
						return i;
				}
				else {
					return i;
				}
			}
			p = null;
		}
		//greater than last cluster in last page
		String filename = t.getStrTableName() + t.getPageCount();
		String path = "data/" + filename;
		
		Page p = (Page) deserialize(path);
		Vector<Tuple> rows = p.getRows();
		
		if (rows.size() == getPageMax()) {
			Page pNew = new Page();
			t.setPageCount(t.getPageCount()+1);
			pNew.setPageNum(t.getPageCount());
			filename = t.getStrTableName() + t.getPageCount();
			path = "data/" + filename;
			serialize(path, pNew);
			p = null;
			return t.getPageCount();
		}
		else {
			return t.getPageCount();
		}
	}

	public int getPage(Table t, Object clusterKey) throws IOException {
		
		int i = 1;
		
		while (i<=t.getPageCount()) {
			String filename = t.getStrTableName() + i;
			String path = "data/" + filename;
			
			Page p = (Page) deserialize(path);
			Vector<Tuple> rows = p.getRows();
				
			if (t.getClusterType().equals("Integer")) {
				int high = (int) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if ((int)clusterKey > high) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("Double")) {
				double high = (double) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if ((double)clusterKey > high) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("String")) {
				String high = (String) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high.compareTo((String)clusterKey) < 0) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("Date")) {
				Date high = (Date) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high.compareTo((Date)clusterKey) < 0) {
					p = null;
					i++;
				}
				else {
					return i;
				}
			}
			else if (t.getClusterType().equals("Polygon")) {
				PolygonC high = new PolygonC ((Polygon)rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn()));
				if (high.compareTo(((Polygon) clusterKey)) < 0) {
					p = null;
					i++;
				}
				else {
					//System.out.println(high.compareTo(((Polygon)clusterKey)));
					return i;
				}
			}
			else if (t.getClusterType().equals("Boolean")) {
				boolean high = (boolean) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high == false) {
					if ((boolean)clusterKey == true) {
						p = null;
						i++;
					}
					else
						return i;
				}
				else {
					return i;
				}
			}
			p = null;
		}
		//greater than last cluster in last page
		String filename = t.getStrTableName() + t.getPageCount();
		String path = "data/" + filename;
		
		Page p = (Page) deserialize(path);
		Vector<Tuple> rows = p.getRows();
		
		if (rows.size() == getPageMax()) {
			Page pNew = new Page();
			t.setPageCount(t.getPageCount()+1);
			pNew.setPageNum(t.getPageCount());
			filename = t.getStrTableName() + t.getPageCount();
			path = "data/" + filename;
			serialize(path, pNew);
			p = null;
			return t.getPageCount();
		}
		else {
			return t.getPageCount();
		}
	}

	
	public int getIndexPage(Table t, int page, Object clusterKey) {
		
		boolean entered = false;
		
		String filename = t.getStrTableName() + page;
		String path = "data/" + filename;
		
		Page p = (Page) deserialize(path);
		Vector<Tuple> rows = p.getRows();
		
		if (rows.size() == 0) {
			entered = true;
			return 0;
		}
		
		else {
			if (t.getClusterType().equals("Integer")) {
				if ((int) clusterKey > (int) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn())) {
					p = null;
					entered = true;
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("Double")) {
				if ((double) clusterKey > (double) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn())) {
					p = null;
					entered = true;
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("Date")) {
				if (((Date) clusterKey).compareTo((Date) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn())) > 0) {
					p = null;
					entered = true;
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("Polygon")) {
				if ((new PolygonC((Polygon)clusterKey)).compareTo(((Polygon) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn()))) > 0) {
					p = null;
					entered = true;
					return rows.size();
					}
			}
			else if (t.getClusterType().equals("String")) {
				if (((String) clusterKey).compareTo((String) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn())) > 0) {
					p = null;
					entered = true;
					return rows.size();
				}	
			}
			else if (t.getClusterType().equals("Boolean")) {
				boolean high = (boolean) rows.get(rows.size()-1).getClusterKey(t.getStrClusteringKeyColumn());
				if (high == false) {
					if ((boolean)clusterKey == true) {
						p = null;
						entered = true;
						return rows.size();
					}
				}
			}
		}
		
		if (entered == false) {
		
			if (t.getClusterType().equals("Integer")) {
				if (rows.size() == 1) {
					if ((int) clusterKey <= (int) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn()))
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					int midValue = (int) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if ((int)clusterKey <= midValue) {
							high = mid;
							mid = (low+high)/2;
							midValue = (int) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (int) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					return high;
				}
			}
			else if (t.getClusterType().equals("Double")) {
				if (rows.size() == 1) {
					if ((double) clusterKey <= (double) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn()))
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					double midValue = (double) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if ((double)clusterKey <= midValue) {
							high = mid;
							mid = (low+high)/2;
							midValue = (double) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (double) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
						if (high == (low+1))
							count++;
					}
					p = null;
					return high;
				}
			}
			else if (t.getClusterType().equals("Date")) {
				if (rows.size() == 1) {
					if (((Date) clusterKey).compareTo((Date) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn())) <= 0)
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					Date midValue = (Date) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if (((Date)clusterKey).compareTo(midValue) <= 0) {
							high = mid;
							mid = (low+high)/2;
							midValue = (Date) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (Date) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					return high;
				}
			}
			else if (t.getClusterType().equals("Polygon")) {
				if (rows.size() == 1) {
					if ((new PolygonC((Polygon)clusterKey)).compareTo(((Polygon) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn()))) <= 0)
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					Polygon midValue = ((Polygon) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn()));
					int count = 0;
					while (count<2 && high != low) {
						if ((new PolygonC((Polygon)clusterKey)).compareTo(midValue) <= 0) {
							high = mid;
							mid = (low+high)/2;
							midValue = ((Polygon) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn()));
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = ((Polygon) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn()));
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					return high;
				}
			}
			else if (t.getClusterType().equals("String")) {
				if (rows.size() == 1) {
					if (((String) clusterKey).compareTo((String) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn())) <= 0)
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					String midValue = (String) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if (((String)clusterKey).compareTo(midValue) <= 0) {
							high = mid;
							mid = (low+high)/2;
							midValue = (String) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (String) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					return high;
				}
			}
			else if (t.getClusterType().equals("Boolean")) {
				if (rows.size() == 1) {
					boolean c = (boolean) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn());
					if (c == true) {
						if ((boolean)clusterKey == true || (boolean)clusterKey == false) {
							return 0;
						}
						else
							return 1;
					}
					else if (c == false ){
						if ((boolean)clusterKey == false) {
							return 0;
						}
						else
							return 1;
					}
					
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					boolean midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if (midValue == true) {
							if ((boolean)clusterKey == true || (boolean)clusterKey == false) {
								high = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
							else {
								low = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
						}
						else if (midValue == false ){
							if ((boolean)clusterKey == false) {
								high = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
							else {
								low = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					return high;
				}
			}
		}
		return 0;
	}
	
	public int getIndexPageLast(Table t, int page, Object clusterKey) {
		
		boolean entered = false;
		
		if (page > t.getPageCount()) {
			Page pNew = new Page();
			t.setPageCount(t.getPageCount()+1);
			pNew.setPageNum(t.getPageCount());
			String filename = t.getStrTableName() + t.getPageCount();
			String path = "data/" + filename;
			serialize(path, pNew);
			return 0;
		}
		
		String filename = t.getStrTableName() + page;
		String path = "data/" + filename;
		
		Page p = (Page) deserialize(path);
		Vector<Tuple> rows = p.getRows();
		
		if (rows.size() == 0) {
			entered = true;
			return 0;
		}
		
		if (entered == false) {
		
			if (t.getClusterType().equals("Integer")) {
				if (rows.size() == 1) {
					if ((int) clusterKey < (int) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn()))
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					int midValue = (int) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if ((int)clusterKey <= midValue) {
							high = mid;
							mid = (low+high)/2;
							midValue = (int) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (int) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					
					//high = first occurrance but we want last
					for (int j=high; j<rows.size(); j++) {
						if ((int) rows.get(j).getClusterKey(t.getStrClusteringKeyColumn()) > (int)clusterKey) 
							return j;
					}
					return rows.size();
					
				}
			}
			else if (t.getClusterType().equals("Double")) {
				if (rows.size() == 1) {
					if ((double) clusterKey <= (double) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn()))
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					double midValue = (double) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if ((double)clusterKey <= midValue) {
							high = mid;
							mid = (low+high)/2;
							midValue = (double) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (double) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
						if (high == (low+1))
							count++;
					}
					p = null;
					//high = first occurrance but we want last
					for (int j=high; j<rows.size(); j++) {
						if ((double) rows.get(j).getClusterKey(t.getStrClusteringKeyColumn()) > (double)clusterKey) 
							return j;
					}
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("Date")) {
				if (rows.size() == 1) {
					if (((Date) clusterKey).compareTo((Date) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn())) < 0)
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					Date midValue = (Date) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if (((Date)clusterKey).compareTo(midValue) <= 0) {
							high = mid;
							mid = (low+high)/2;
							midValue = (Date) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (Date) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					//high = first occurrance but we want last
					for (int j=high; j<rows.size(); j++) {
						if (((Date) rows.get(j).getClusterKey(t.getStrClusteringKeyColumn())).compareTo((Date)clusterKey) > 0) 
							return j;
					}
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("Polygon")) {
				if (rows.size() == 1) {
					if ((new PolygonC((Polygon)clusterKey)).compareTo(((Polygon) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn()))) < 0)
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					Polygon midValue = ((Polygon) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn()));
					int count = 0;
					while (count<2 && high != low) {
						if ((new PolygonC((Polygon)clusterKey)).compareTo(midValue) <= 0) {
							high = mid;
							mid = (low+high)/2;
							midValue = ((Polygon) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn()));
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = ((Polygon) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn()));
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					for (int j=high; j<rows.size(); j++) {
						if ((new PolygonC((Polygon) rows.get(j).getClusterKey(t.getStrClusteringKeyColumn()))).compareTo((Polygon)clusterKey) > 0) 
							return j;
					}
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("String")) {
				if (rows.size() == 1) {
					if (((String) clusterKey).compareTo((String) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn())) < 0)
						return 0;
					else
						return 1;
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					String midValue = (String) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if (((String)clusterKey).compareTo(midValue) <= 0) {
							high = mid;
							mid = (low+high)/2;
							midValue = (String) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						else {
							low = mid;
							mid = (low+high)/2;
							midValue = (String) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					for (int j=high; j<rows.size(); j++) {
						if (((String) rows.get(j).getClusterKey(t.getStrClusteringKeyColumn())).compareTo((String)clusterKey) > 0) 
							return j;
					}
					return rows.size();
				}
			}
			else if (t.getClusterType().equals("Boolean")) {
				if (rows.size() == 1) {
					boolean c = (boolean) rows.get(0).getClusterKey(t.getStrClusteringKeyColumn());
					if (c == true) {
						if ((boolean)clusterKey == true || (boolean)clusterKey == false) {
							return 0;
						}
						else
							return 1;
					}
					else if (c == false ){
						return 1;
					}
					
				}
				else {
					int low = 0;
					int high = rows.size()-1;
					int mid = (low+high)/2;
					boolean midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
					int count = 0;
					while (count<2 && high != low) {
						if (midValue == true) {
							if ((boolean)clusterKey == true || (boolean)clusterKey == false) {
								high = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
							else {
								low = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
						}
						else if (midValue == false ){
							if ((boolean)clusterKey == false) {
								high = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
							else {
								low = mid;
								mid = (low+high)/2;
								midValue = (boolean) rows.get(mid).getClusterKey(t.getStrClusteringKeyColumn());
							}
						}
						if (high == (low+1))
							count++;
					}
					p = null;
					//return high;
					
					//high = first occurrance but we want last
					if ((boolean)clusterKey == false) {

						for (int j=high; j<rows.size(); j++) {
							if ((boolean) rows.get(j).getClusterKey(t.getStrClusteringKeyColumn()) != false) 
								return j;
						}
						return rows.size();
					}
					else {
						return rows.size();
					}
				}
			}
		}
		return 0;
	}

	public void fixRest (Table t, Tuple tup, int page) throws IOException {
		
		Hashtable<String, String> htblColBIndex = t.getHtblColBIndex();
		Hashtable<String, String> htblColRIndex = t.getHtblColRIndex();
		
		int oldPage = page-1;
		String oldPath = "data/" + t.getStrTableName() + oldPage;
		
		if (page <= t.getPageCount()) {
			
			String filename = t.getStrTableName() + page;
			String path = "data/" + filename;
			
			Page p = (Page) deserialize(path);
			Vector<Tuple> rows = p.getRows();
			
			if (rows.size() == getPageMax()) {
				Tuple tupExtra = rows.remove(rows.size()-1);
				rows.insertElementAt(tup, 0);
				
				serialize(path, p);
				p = null;

				page++;
				fixRest(t, tupExtra, page);
			}
			else { 
				rows.insertElementAt(tup, 0);
				serialize(path, p);
				p = null;
			}
			
			//update all indexes
			Set <String> keysIndex1 = htblColBIndex.keySet();
			
			for (String key: keysIndex1) {
				String colName = key;
				BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				indexB.update(indexKey, oldPath, path);
				serialize(indexB.getTreePath(), indexB);
			}
				
			Set <String> keysIndex2 = htblColRIndex.keySet();
				
			for (String key: keysIndex2) {
				String colName = key;
				RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				indexR.update(indexKey, oldPath, path);
				serialize(indexR.getTreePath(), indexR);
			}	
		}
		
		else {
			Page p = new Page();
			p.getRows().add(tup);
			p.setPageNum(page);

			String filename = t.getStrTableName() + page;
			String path = "data/" + filename;
			serialize(path, p);
			p = null;
			
			t.setPageCount(t.getPageCount()+1);
			
			//update all indexes
			Set <String> keysIndex1 = htblColBIndex.keySet();
			
			for (String key: keysIndex1) {
				String colName = key;
				BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				indexB.update(indexKey, oldPath, path);
				serialize(indexB.getTreePath(), indexB);
			}
				
			Set <String> keysIndex2 = htblColRIndex.keySet();
				
			for (String key: keysIndex2) {
				String colName = key;
				RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				indexR.update(indexKey, oldPath, path);
				serialize(indexR.getTreePath(), indexR);
			}	
		}
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {
		
		checkTableName (strTableName);
		
		try {
			boolean check = checkDataTypes(strTableName, htblColNameValue);
			if (check == false)
				throw new DBAppException ("Type mismatch");
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		checkAllCols(strTableName, htblColNameValue);
		
		
		String tablePath = "data/" + strTableName;
		Table t = (Table) deserialize(tablePath);
		
		Tuple tup = new Tuple (htblColNameValue);
		
		Hashtable<String, String> htblColBIndex = t.getHtblColBIndex();
		Hashtable<String, String> htblColRIndex = t.getHtblColRIndex();
			
		//if table has no pages
		if (t.getPageCount() == 0) {
			Page p = new Page ();
			p.getRows().add(tup);
			t.setPageCount(t.getPageCount()+1);
			p.setPageNum(t.getPageCount());
			serialize(tablePath, t);
			//serialize page
			String filename = strTableName + "1";
			String path = "data/" + filename;
			serialize(path, p);
			p = null;
			
			
			//add to all indexes
			Set <String> keys = htblColBIndex.keySet();
				
			for (String key: keys) {
				String colName = key;
				BTree index = (BTree) deserialize(htblColBIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				index.insert(indexKey, path);
				serialize(index.getTreePath(), index);
			}
				
			Set <String> keys2 = htblColRIndex.keySet();
				
			for (String key: keys2) {
				String colName = key;
				RTree index = (RTree) deserialize(htblColRIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				index.insert(indexKey, path);
				serialize(index.getTreePath(), index);
			}
				
		}
		
		//table already has pages
		else {
			
			//get clusterKey value
			Object clusterKey = null;
			Set <String> keys = htblColNameValue.keySet();
			for (String key: keys) {
				if (key.equals(t.getStrClusteringKeyColumn()))
					clusterKey = htblColNameValue.get(key);
			}
			
			int page;
			
			String indexPath = t.indexExists(t.getStrClusteringKeyColumn(), t.getClusterType());
			
			if (indexPath != null) {
				
				Object indexKey = clusterKey;
			
				if (t.getClusterType().equals("Polygon")) {
					RTree index = (RTree) deserialize(indexPath);
					page = index.findTuplePageLast(indexKey);
					
					String filename = t.getStrTableName() + page;
					String path = "data/" + filename;
					Page p = (Page) deserialize(path);
					PolygonC newCluster = new PolygonC((Polygon) clusterKey);
					if (p.getRows().size() == getPageMax() && newCluster.compareTo(p.getRows().lastElement().getClusterKey(t.getStrClusteringKeyColumn())) <= 0)
						page = page+1;
					
					index = null;
				}
				else {
					BTree index = (BTree) deserialize(indexPath);
					page = index.findTuplePageLast(indexKey);
					
					String filename = t.getStrTableName() + page;
					String path = "data/" + filename;
					Page p = (Page) deserialize(path);
					if (p.getRows().size() == getPageMax() && compareAll(t.getStrTableName(), p.getRows().lastElement().getClusterKey(t.getStrClusteringKeyColumn()), (clusterKey)) <=0)
						page = page+1;
					
					index = null;
				}
				
			}
			
			else
				page = getPageLast(t, clusterKey);
				
			int index = getIndexPageLast(t, page, clusterKey);
				
			String filename = t.getStrTableName() + page;
			String path = "data/" + filename;
			Page p = (Page) deserialize(path);
			
			//space available
			if (p.getRows().size() != getPageMax()) {
				p.getRows().insertElementAt(tup, index);
				serialize(path, p);
				p = null;
			}
			
			//no space available
			else {
				Tuple tupExtra = p.getRows().remove(p.getRows().size()-1);
				p.getRows().insertElementAt(tup, index);
				serialize(path, p);
				page++;
				fixRest (t, tupExtra, page);
			}
			
			//add to all indexes
			Set <String> keysIndex1 = htblColBIndex.keySet();
			
			for (String key: keysIndex1) {
				String colName = key;
				BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				indexB.insert(indexKey, path);
				serialize(indexB.getTreePath(), indexB);
			}
				
			Set <String> keysIndex2 = htblColRIndex.keySet();
				
			for (String key: keysIndex2) {
				String colName = key;
				RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
				Object indexKey = tup.getColKey(colName);
				indexR.insert(indexKey, path);
				serialize(indexR.getTreePath(), indexR);
			}
			
		}
		
		serialize(tablePath, t);
		t = null;
	}
	
	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue ) throws DBAppException, IOException, ParseException {
		
		checkTableName (strTableName);
		
		try {
			boolean check = checkDataTypes(strTableName, htblColNameValue);
			if (check == false)
				throw new DBAppException ("Type mismatch");
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		String tablePath = "data/" + strTableName;
		Table t = (Table) deserialize(tablePath);
		
		//parse clusterKey to correct format
		int page = 0;
		Object clusterKey = null;
		
		if (t.getClusterType().equals("Integer")) {
			clusterKey = Integer.parseInt(strClusteringKey);
		}
		else if (t.getClusterType().equals("Double")) {
			clusterKey = Double.parseDouble(strClusteringKey);
		}
		else if (t.getClusterType().equals("String")) {
			clusterKey = strClusteringKey;
		}
		else if (t.getClusterType().equals("Date")) {
			clusterKey = new SimpleDateFormat("YYYY-MM-DD").parse(strClusteringKey);
		}
		else if (t.getClusterType().equals("Boolean")) {
			clusterKey = Boolean.parseBoolean(strClusteringKey);
		}
		else if (t.getClusterType().equals("Polygon")) {
			clusterKey = new Polygon();
			StringTokenizer st = new StringTokenizer(strClusteringKey, "(),");  
		    while (st.hasMoreTokens()) {  
		        ((Polygon) clusterKey).addPoint(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));  
		    }
		}
		
		String indexPath = t.indexExists(t.getStrClusteringKeyColumn(), t.getClusterType());
		
		if (indexPath != null) {
			
			Object indexKey = clusterKey;
		
			if (t.getClusterType().equals("Polygon")) {
				RTree index = (RTree) deserialize(indexPath);
				page = index.findTuplePageFirst(indexKey);
				index = null;
			}
			else {
				BTree index = (BTree) deserialize(indexPath);
				page = index.findTuplePageFirst(indexKey);
				index = null;
			}
			
			if (page==0)
				return;
		}
		
		else 
			page = getPage(t, clusterKey);
		
		int index = getIndexPage(t, page, clusterKey);
		
		String filename = t.getStrTableName() + page;
		String path = "data/" + filename;
		
		Page p = (Page) deserialize(path);
		Vector<Tuple> rows = p.getRows();
		
		Hashtable<String, String> htblColBIndex = t.getHtblColBIndex();
		Hashtable<String, String> htblColRIndex = t.getHtblColRIndex();
		
		int i = index;
		
		while (i < rows.size() && equalsP(rows.get(i).getClusterKey(t.getStrClusteringKeyColumn()), (clusterKey))) {
			Set <String> keys = htblColNameValue.keySet();
			Tuple tup = rows.get(i);
			Hashtable<String, Object> rowOld = rows.get(i).getHtblColNameData();
			
			//delete from all indexes
			for (String key1: keys) {
			
				Set <String> keysIndex1 = htblColBIndex.keySet();
				
				for (String key: keysIndex1) {
					if (key1.equals(key)) {
						String colName = key;
						BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
						Object indexKey = tup.getColKey(colName);
						indexB.delete(indexKey, path);
						serialize(indexB.getTreePath(), indexB);
					}
				}
					
				Set <String> keysIndex2 = htblColRIndex.keySet();
					
				for (String key: keysIndex2) {
					if (key1.equals(key)) {
						String colName = key;
						RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
						Object indexKey = tup.getColKey(colName);
						indexR.delete(indexKey, path);
						serialize(indexR.getTreePath(), indexR);
					}
				}
			}
			//
			
			for (String key2: keys) {
				rowOld.replace(key2, htblColNameValue.get(key2));
				rowOld.replace("TouchDate", new Date(System.currentTimeMillis()));
				
				//add to all indexes
				Set <String> keysIndex1 = htblColBIndex.keySet();
					
				for (String key: keysIndex1) {
					if (key2.equals(key)) {
						BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
						indexB.insert(htblColNameValue.get(key2), path);
						serialize(indexB.getTreePath(), indexB);
					}
				}
						
				Set <String> keysIndex2 = htblColRIndex.keySet();
						
				for (String key: keysIndex2) {
					if (key2.equals(key)) {
						RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
						indexR.insert(htblColNameValue.get(key2), path);
						serialize(indexR.getTreePath(), indexR);
					}
				}
				//
				
			}
			i++;
		}
		
		serialize(path, p);
		p = null;
		
		for (int j=page+1; j<=t.getPageCount(); j++) {
		
			filename = t.getStrTableName() + j;
			path = "data/" + filename;
			
			p = (Page) deserialize(path);
			rows = p.getRows();
			
			for (int k=0; k<rows.size(); k++) {
				if (!(equalsP(rows.get(k).getClusterKey(t.getStrClusteringKeyColumn()), (clusterKey))))
					return;
				else {
					Set <String> keys = htblColNameValue.keySet();
					Tuple tup = rows.get(k);
					Hashtable<String, Object> rowOld = rows.get(k).getHtblColNameData();
					
					//delete from all indexes
					for (String key1: keys) {
					
						Set <String> keysIndex1 = htblColBIndex.keySet();
						
						for (String key: keysIndex1) {
							if (key1.equals(key)) {
								String colName = key;
								BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
								Object indexKey = tup.getColKey(colName);
								indexB.delete(indexKey, path);
								serialize(indexB.getTreePath(), indexB);
							}
						}
							
						Set <String> keysIndex2 = htblColRIndex.keySet();
							
						for (String key: keysIndex2) {
							if (key1.equals(key)) {
								String colName = key;
								RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
								Object indexKey = tup.getColKey(colName);
								indexR.delete(indexKey, path);
								serialize(indexR.getTreePath(), indexR);
							}
						}
					}
					//
					
					for (String key2: keys) {
						rowOld.replace(key2, htblColNameValue.get(key2));
						rowOld.replace("TouchDate", new Date(System.currentTimeMillis()));
						
						//add to all indexes
						Set <String> keysIndex1 = htblColBIndex.keySet();
							
						for (String key: keysIndex1) {
							if (key2.equals(key)) {
								BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
								indexB.insert(htblColNameValue.get(key2), path);
								serialize(indexB.getTreePath(), indexB);
							}
						}
								
						Set <String> keysIndex2 = htblColRIndex.keySet();
								
						for (String key: keysIndex2) {
							if (key2.equals(key)) {
								RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
								indexR.insert(htblColNameValue.get(key2), path);
								serialize(indexR.getTreePath(), indexR);
							}
						}
						//
					}
				}
			}
			serialize(path, p);
			p = null;
		}
		t = null;
	}
	
	public void deleteTable(String strTableName, Hashtable<String, Object> htblColNameValue ) throws DBAppException, IOException {
		
		checkTableName (strTableName);
		
		try {
			boolean check = checkDataTypes(strTableName, htblColNameValue);
			if (check == false)
				throw new DBAppException ("Type mismatch");
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		String tablePath = "data/" + strTableName;
		Table t = (Table) deserialize(tablePath);
		
		Hashtable<String, String> htblColBIndex = t.getHtblColBIndex();
		Hashtable<String, String> htblColRIndex = t.getHtblColRIndex();
		
		Boolean useIndex = false;
		
		Set <String> keys = htblColNameValue.keySet();
		Vector<String> results = new Vector<String>();
		
		for (String key: keys) {
			
			String indexPath = t.indexExists(key, t.getColType(key));
			
			if (indexPath != null) {
				
				useIndex = true;
				
				Object indexKey = htblColNameValue.get(key);
			
				if (t.getColType(key).equals("Polygon")) {
					
					RTree index = (RTree) deserialize(indexPath);
					Vector<String> pages = index.findTuplePageD(indexKey);
					
					if (pages == null)
						return;
					
					Vector<String> newRes = new Vector<String>();
					
					//get all pages
					for (int i=0; i<pages.size(); i++) {
						if (results.size() == 0)
							newRes.add(pages.get(i));
						else {
							for(int j=0; j<results.size(); j++) {
								if (results.get(j).equals(pages.get(i)))
									newRes.add(pages.get(i));
							}
						}
					}
					if (newRes.size() == 0)
						return;
					else
						results = newRes;
					
					index = null;
				}
				else {
					BTree index = (BTree) deserialize(indexPath);
					Vector<String> pages = index.findTuplePageD(indexKey);
					
					if (pages == null)
						return;
					
					Vector<String> newRes = new Vector<String>();
					
					//get all pages
					for (int i=0; i<pages.size(); i++) {
						if (results.size() == 0)
							newRes.add(pages.get(i));
						else {
							for(int j=0; j<results.size(); j++) {
								if (results.get(j).equals(pages.get(i)))
									newRes.add(pages.get(i));
							}
						}
					}
					if (newRes.size() == 0)
						return;
					else
						results = newRes;
					
					index = null;
				}
				
				if (results.size() == 0)
					return;
			}
		}
		
		//filter the page results from duplicates
		if (results.size() != 0) {
			Vector<String> noDup = new Vector<String>();
			noDup.add(results.get(0));
			for (int i=1; i<results.size(); i++) {
				
				String x = results.get(i);
				Boolean found = false;
				for (int j=0; j<noDup.size(); j++) {
					if (noDup.get(j).equals(x)) {
						found = true;
						break;
					}
				}
				if (!(found))
					noDup.add(x);
			}
			results = noDup;
		}
		
		Object clusterHash = null;
		
		for (String key: keys) {
			if (key.equals(t.getStrClusteringKeyColumn()))
				clusterHash = htblColNameValue.get(key);
		}
		
		
		if (useIndex) {
			
			for (int i=0; i<results.size(); i++) {
				
				String path = results.get(i);
				
				Page p = (Page) deserialize(path);
				Vector<Tuple> rows = p.getRows();
				
				String filename = t.getStrTableName() + p.getPageNum();
				
				int j = 0;
				
				if (clusterHash != null) {
					j = getIndexPage(t, p.getPageNum(), clusterHash);
				}
				
				while (j<rows.size()) {
					
					boolean flag = true;
					
					Hashtable<String, Object> tupleData = rows.get(j).getHtblColNameData();
					
					Set <String> keys1 = htblColNameValue.keySet();
					
					for (String key1: keys1) {
						
						Set <String> keys2 = tupleData.keySet();
						
						for (String key2: keys2) {
							if (key1.equals(key2) && !(equalsP((htblColNameValue.get(key1)), (tupleData.get(key1))))) {
								flag = false;
								break;
							}
						}
					}
					
					if (flag) {	
						if (rows.size() == 1) {
							
							Tuple tup = rows.remove(j);
							
							//delete from all indexes
							for (String key1: keys) {
							
								Set <String> keysIndex1 = htblColBIndex.keySet();
								
								for (String key: keysIndex1) {
									if (key1.equals(key)) {
										String colName = key;
										BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexB.delete(indexKey, path);
										serialize(indexB.getTreePath(), indexB);
									}
								}
									
								Set <String> keysIndex2 = htblColRIndex.keySet();
									
								for (String key: keysIndex2) {
									if (key1.equals(key)) {
										String colName = key;
										RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexR.delete(indexKey, path);
										serialize(indexR.getTreePath(), indexR);
									}
								}
							}
							//
							
							t.setPageCount(t.getPageCount()-1);
							new File (path).delete();
						}
						else {
							Tuple tup = rows.remove(j);
							
							//delete from all indexes
							for (String key1: keys) {
							
								Set <String> keysIndex1 = htblColBIndex.keySet();
								
								for (String key: keysIndex1) {
									if (key1.equals(key)) {
										String colName = key;
										BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexB.delete(indexKey, path);
										serialize(indexB.getTreePath(), indexB);
									}
								}
									
								Set <String> keysIndex2 = htblColRIndex.keySet();
									
								for (String key: keysIndex2) {
									if (key1.equals(key)) {
										String colName = key;
										RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexR.delete(indexKey, path);
										serialize(indexR.getTreePath(), indexR);
									}
								}
							}
							//
							
							j--;
							filename = t.getStrTableName() + p.getPageNum();
							path = "data/" + filename;
							serialize(path, p);
							p = null;
						}
					}
					j++;
				}
				
				
			}
			
			
		}
		
		
		else {
		
			for (int i=1; i<=t.getPageCount(); i++) {
				
				String filename = t.getStrTableName() + i;
				String path = "data/" + filename;
				
				Page p = (Page) deserialize(path);
				Vector<Tuple> rows = p.getRows();
				
				int j = 0;
				
				if (clusterHash != null) {
					j = getIndexPage(t, p.getPageNum(), clusterHash);
				}
				
				while (j<rows.size()) {
						
					boolean flag = true;
					
					Hashtable<String, Object> tupleData = rows.get(j).getHtblColNameData();
					
					Set <String> keys1 = htblColNameValue.keySet();
					
					for (String key1: keys1) {
						
						Set <String> keys2 = tupleData.keySet();
						
						for (String key2: keys2) {
							if (key1.equals(key2) && !(equalsP((htblColNameValue.get(key1)), (tupleData.get(key1))))) {
								flag = false;
								break;
							}
						}
					}
					
					if (flag) {	
						if (rows.size() == 1) {
							
							Tuple tup = rows.remove(j);
							
							//delete from all indexes
							for (String key1: keys) {
							
								Set <String> keysIndex1 = htblColBIndex.keySet();
								
								for (String key: keysIndex1) {
									if (key1.equals(key)) {
										String colName = key;
										BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexB.delete(indexKey, path);
										serialize(indexB.getTreePath(), indexB);
									}
								}
									
								Set <String> keysIndex2 = htblColRIndex.keySet();
									
								for (String key: keysIndex2) {
									if (key1.equals(key)) {
										String colName = key;
										RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexR.delete(indexKey, path);
										serialize(indexR.getTreePath(), indexR);
									}
								}
							}
							//
							
							t.setPageCount(t.getPageCount()-1);
							new File (path).delete();
						}
						else {
							Tuple tup = rows.remove(j);
							
							//delete from all indexes
							for (String key1: keys) {
							
								Set <String> keysIndex1 = htblColBIndex.keySet();
								
								for (String key: keysIndex1) {
									if (key1.equals(key)) {
										String colName = key;
										BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexB.delete(indexKey, path);
										serialize(indexB.getTreePath(), indexB);
									}
								}
									
								Set <String> keysIndex2 = htblColRIndex.keySet();
									
								for (String key: keysIndex2) {
									if (key1.equals(key)) {
										String colName = key;
										RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
										Object indexKey = tup.getColKey(colName);
										indexR.delete(indexKey, path);
										serialize(indexR.getTreePath(), indexR);
									}
								}
							}
							//
							
							j--;
							filename = t.getStrTableName() + i;
							path = "data/" + filename;
							serialize(path, p);
							p = null;
						}
					}
					j++;
				}
			}
		}
		
		serialize(tablePath, t);
		t = null;
	}
	
	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		
		checkTableName (strTableName);
		checkColExists(strTableName, strColName);
		
		Table t = (Table) deserialize("data/" + strTableName);
		BTree index = new BTree (this.getNodeMax(), strTableName, strColName, t.getColType(strColName));
		
		for (int i=1; i<=t.getPageCount(); i++) {
			
			String filename = t.getStrTableName() + i;
			String path = "data/" + filename;
			
			Page p = (Page) deserialize(path);
			Vector<Tuple> rows = p.getRows();
			
			for (int j=0; j<rows.size(); j++) {
				Tuple tup = rows.get(j);
				Object indexKey = tup.getColKey(strColName);
				index.insert(indexKey, "data/" + strTableName + i);
				
			}
			
		}
		
		serialize(index.getTreePath(), index);
		t.getHtblColBIndex().put(strColName, index.getTreePath());
		serialize("data/" + strTableName, t);
		
		index = null;
		t = null;
		
		//update metadata with indexed = true
		BufferedWriter writer = new BufferedWriter(new FileWriter("data/metadata1.csv", true));
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		
		String line = br.readLine();
		while (line != null) {
			String[] info = line.split(",");
				
			if (info[0].equals(strTableName) && info[1].equals(strColName)) {
				info[4] = "True";
				writer.write(info[0] + "," + info[1] + "," + info[2] + "," + info[3] + "," + info[4]);
				writer.newLine();
			}
				
			else {
				writer.write(line);
				writer.newLine();
			}
				
			line = br.readLine();
		}
		br.close();
		writer.close();
		
		File fOld = new File("data/metadata.csv");
		fOld.delete();
		
		File fNew = new File("data/metadata1.csv");
		fNew.renameTo(new File("data/metadata.csv"));
		
	}
			
	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException, IOException {
		
		checkTableName (strTableName);
		checkColExists(strTableName, strColName);
		
		Table t = (Table) deserialize("data/" + strTableName);
		RTree index = new RTree (this.getNodeMax(), strTableName, strColName, t.getColType(strColName));
		
		for (int i=1; i<=t.getPageCount(); i++) {
			
			String filename = t.getStrTableName() + i;
			String path = "data/" + filename;
			
			Page p = (Page) deserialize(path);
			Vector<Tuple> rows = p.getRows();
			
			for (int j=0; j<rows.size(); j++) {
				Tuple tup = rows.get(j);
				Object indexKey = tup.getColKey(strColName);
				index.insert(indexKey, "data/" + strTableName + i);
				
			}
			
		}
		
		serialize(index.getTreePath(), index);
		t.getHtblColRIndex().put(strColName, index.getTreePath());
		serialize("data/" + strTableName, t);
		index = null;
		t = null;	
		
		//update metadata with indexed = true
		BufferedWriter writer = new BufferedWriter(new FileWriter("data/metadata1.csv", true));
		
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		
		String line = br.readLine();
		while (line != null) {
			String[] info = line.split(",");
				
			if (info[0].equals(strTableName) && info[1].equals(strColName)) {
				info[4] = "True";
				writer.write(info[0] + "," + info[1] + "," + info[2] + "," + info[3] + "," + info[4]);
				writer.newLine();
			}
				
			else {
				writer.write(line);
				writer.newLine();
			}
				
			line = br.readLine();
		}
		br.close();
		writer.close();
		
		File fOld = new File("data/metadata.csv");
		fOld.delete();
		
		File fNew = new File("data/metadata1.csv");
		fNew.renameTo(new File("data/metadata.csv"));
	}
	
	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		Vector<Tuple> results = new Vector<Tuple>();
		
		Boolean linear = true;
		
		//all columns are not indexed
		for (int i=0; i<arrSQLTerms.length; i++) {
			
			String tableName = arrSQLTerms[i]._strTableName;
			Table t = (Table) deserialize("data/" + tableName);
			String columnName = arrSQLTerms[i]._strColumnName;
			String index = t.indexExists(columnName, t.getColType(columnName));
			
			if (index != null) {
				linear = false;
				break;
			}
			
		}

		//any column around or/xor is not indexed
		for (int i=0; i<arrSQLTerms.length; i++) {
			
			String tableName = arrSQLTerms[i]._strTableName;
			Table t = (Table) deserialize("data/" + tableName);
			String columnName = arrSQLTerms[i]._strColumnName;
			String index = t.indexExists(columnName, t.getColType(columnName));
			
			if (index == null) {
				
				if (i<strarrOperators.length) {
					
					if (i==0) {
						
						if (strarrOperators[i].equalsIgnoreCase("or") || strarrOperators[i].equalsIgnoreCase("xor")) {
							linear = true;
							break;
						}
					}
					else {
						
						if (strarrOperators[i].equalsIgnoreCase("or") || strarrOperators[i].equalsIgnoreCase("xor") || strarrOperators[i-1].equalsIgnoreCase("or") || strarrOperators[i-1].equalsIgnoreCase("xor")) {
							linear = true;
							break;
						}
					}
				}
				
				else {
					
					if (strarrOperators[i-1].equalsIgnoreCase("or") || strarrOperators[i-1].equalsIgnoreCase("xor")) {
						linear = true;
						break;
					}
				}
			}
			
		}
		
		if (linear) {
			return selectLinear(arrSQLTerms, strarrOperators);
		}
		
		else {
			
			int i=0;
			while (i<arrSQLTerms.length) {
			
				String tableName = arrSQLTerms[i]._strTableName;
				String columnName = arrSQLTerms[i]._strColumnName;
				String strOperator = arrSQLTerms[i]._strOperator;
				Object objValue = arrSQLTerms[i]._objValue;
				
				if (i == 0) {
					
					if (strarrOperators.length == 0) {
						results = select(tableName, columnName, strOperator, objValue);
						i++;
					}
					else {

						if (strarrOperators[i].equalsIgnoreCase("and")) {				
				
							Table t = (Table) deserialize("data/" + tableName);
							String index = t.indexExists(columnName, t.getColType(columnName));
							
							if (index != null) {
								results = select(tableName, columnName, strOperator, objValue);
								i++;
							}
							
							else {
								int iNew = 0;
								Vector<SQLTerm> extrasSQL = new Vector<SQLTerm>();
								for (int j=i; j<arrSQLTerms.length; j++) {
									
									tableName = arrSQLTerms[j]._strTableName;
									columnName = arrSQLTerms[j]._strColumnName;
									t = (Table) deserialize("data/" + tableName);
									index = t.indexExists(columnName, t.getColType(columnName));
									
									if (index != null) {
										extrasSQL.add(0, arrSQLTerms[j]);
										iNew = j+1;
										break;
									}
									else {
										extrasSQL.add(0, arrSQLTerms[j]);
									}
								}
								results = selectHelper(t, extrasSQL);
								i = iNew;	
							}

						}
						else {
							results = select(tableName, columnName, strOperator, objValue);
							i++;
						}
					}
				}
				
				else {
					
					Vector<Tuple> resultsTemp = new Vector<Tuple>();
				
					if (strarrOperators[i-1].equalsIgnoreCase("and")) {
										
						Table t = (Table) deserialize("data/" + tableName);
						String index = t.indexExists(columnName, t.getColType(columnName));
						
						if (index!=null) {
							resultsTemp = select(tableName, columnName, strOperator, objValue);
							results = intersect(results, resultsTemp);
						}
						else
							results = selectFrom(t, results, columnName, strOperator, objValue);
					}
					else if (strarrOperators[i-1].equalsIgnoreCase("or")) {
						resultsTemp = select(tableName, columnName, strOperator, objValue);
						for (int j=0; j<resultsTemp.size(); j++)
							results.add(resultsTemp.get(j));
					}
					else {
						resultsTemp = select(tableName, columnName, strOperator, objValue);
						results = xor(results, resultsTemp);
					}
					
					i++;
				}
			}
			
			//filter the results from duplicates
			Vector<Tuple> noDup = new Vector<Tuple>();
			if (results.size() != 0) {
				noDup.add(results.get(0));
				for (int i1=1; i1<results.size(); i1++) {
					
					Tuple tup1 = results.get(i1);
					Hashtable<String, Object> values1 = tup1.getHtblColNameData();
					Set <String> keys1 = values1.keySet();
					
					Boolean found = false;
					
					for (int j=0; j<noDup.size(); j++) {
						
						Tuple tup2 = noDup.get(j);
						Hashtable<String, Object> values2 = tup2.getHtblColNameData();
						Boolean equal = true;
						
						for (String key: keys1) {
							if (!(equalsP(values1.get(key), values2.get(key)))) {
								equal = false;
								break;
							}
						}
						if (equal) {
							found = true;
							break;
						}
					}
					
					if (!(found))
						noDup.add(tup1);
				}
				results = noDup;
			}
			
			
			return results.iterator();
		}
	}
	
	//btedkhol hena bas law indexed
	public Vector<Tuple> select(String strTableName, String strColName, String strOperator, Object objValue) {
		
		Table t = (Table) deserialize("data/" + strTableName);
		String indexPath = t.indexExists(strColName, t.getColType(strColName));
		
		Vector<String> pages = new Vector<String>();
		Vector<Tuple> results = new Vector<Tuple>();
		
		Object indexKey = objValue;
		
		if (t.getColType(strColName).equals("Polygon")) {
			RTree index = (RTree) deserialize(indexPath);
			
			if (strOperator.equals("="))
				pages = index.findTuplePageD(indexKey);
			else if (strOperator.equals(">"))
				pages = index.findGreater(indexKey);
			else if (strOperator.equals(">="))
				pages = index.findGreaterE(indexKey);
			else if (strOperator.equals("<"))
				pages = index.findLess(indexKey);
			else if (strOperator.equals("<="))
				pages = index.findLessE(indexKey);
			else
				pages = index.findNotEqual(indexKey);
			
			index = null;
		}
		else {
			BTree index = (BTree) deserialize(indexPath);
			
			if (strOperator.equals("="))
				pages = index.findTuplePageD(indexKey);
			else if (strOperator.equals(">"))
				pages = index.findGreater(indexKey);
			else if (strOperator.equals(">="))
				pages = index.findGreaterE(indexKey);
			else if (strOperator.equals("<"))
				pages = index.findLess(indexKey);
			else if (strOperator.equals("<="))
				pages = index.findLessE(indexKey);
			else
				pages = index.findNotEqual(indexKey);
			
			index = null;
		}
		
		if (pages == null) {
			return results;
		}
		
		
		//filter the page results from duplicates
		if (pages.size() != 0) {
			Vector<String> noDup = new Vector<String>();
			noDup.add(pages.get(0));
			for (int i=1; i<pages.size(); i++) {
						
				String x = pages.get(i);
				Boolean found = false;
				for (int j=0; j<noDup.size(); j++) {
					if (noDup.get(j).equals(x)) {
						found = true;
						break;
					}
				}
				if (!(found))
					noDup.add(x);
			}
			pages = noDup;
		}
		
		for (int i=0; i<pages.size(); i++) {
			
			String path = pages.get(i);
			
			Page p = (Page) deserialize(path);
			Vector<Tuple> rows = p.getRows();
			
			for (int j=0; j<rows.size(); j++) {
				
				Tuple tup = rows.get(j);
				
				if (strOperator.equals("=")) {
					Object x = tup.getColKey(strColName);
					if (equalsP(x, (objValue)))
						results.add(tup);
				}
				else if (strOperator.equals(">")) {
					Object x = tup.getColKey(strColName);
					if (compareAll(t.getColType(strColName), x, objValue) > 0)
						results.add(tup);
				}
				else if (strOperator.equals(">=")) {
					Object x = tup.getColKey(strColName);
					if (compareAll(t.getColType(strColName), x, objValue) >= 0)
						results.add(tup);
				}
				else if (strOperator.equals("<")) {
					Object x = tup.getColKey(strColName);
					if (compareAll(t.getColType(strColName), x, objValue) < 0)
						results.add(tup);
				}
				else if (strOperator.equals("<=")) {
					Object x = tup.getColKey(strColName);
					if (compareAll(t.getColType(strColName), x, objValue) <= 0)
						results.add(tup);
				}
				else {
					Object x = tup.getColKey(strColName);
					if (!(equalsP(x, (objValue))))
						results.add(tup);
				}	
			}
		}
		
		return results;
	}
	
	@SuppressWarnings("rawtypes")
	public Iterator selectLinear(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		
		Vector<Tuple> results = new Vector<Tuple>();
		
		String tableName2 = arrSQLTerms[0]._strTableName;
		String strColName2 = arrSQLTerms[0]._strColumnName;
		String strOperator2 = arrSQLTerms[0]._strOperator;
		Object objValue2 = arrSQLTerms[0]._objValue;
		
		Table t = (Table) deserialize("data/" + tableName2);
		
		for (int i=1; i<=t.getPageCount(); i++) {
			
			Page p = (Page) deserialize("data/" + tableName2 + i);
			Vector<Tuple> rows = p.getRows();
			
			for (int j=0; j<rows.size(); j++) {
				
				Boolean add = false;
				
				Tuple tup = rows.get(j);
				
				if (strOperator2.equals("=")) {
					Object x = tup.getColKey(strColName2);
					if (equalsP(x, (objValue2)))
						add = true;
				}
				else if (strOperator2.equals(">")) {
					Object x = tup.getColKey(strColName2);
					if (compareAll(t.getColType(strColName2), x, objValue2) > 0)
						add = true;
				}
				else if (strOperator2.equals(">=")) {
					Object x = tup.getColKey(strColName2);
					if (compareAll(t.getColType(strColName2), x, objValue2) >= 0)
						add = true;
				}
				else if (strOperator2.equals("<")) {
					Object x = tup.getColKey(strColName2);
					if (compareAll(t.getColType(strColName2), x, objValue2) < 0)
						add = true;
				}
				else if (strOperator2.equals("<=")) {
					Object x = tup.getColKey(strColName2);
					if (compareAll(t.getColType(strColName2), x, objValue2) <= 0)
						add = true;
				}
				else {
					Object x = tup.getColKey(strColName2);
					if (!(equalsP(x, (objValue2))))
						add = true;
				}
				
				for (int k=1; k<arrSQLTerms.length; k++) {
				
					String strColName = arrSQLTerms[k]._strColumnName;
					String strOperator = arrSQLTerms[k]._strOperator;
					Object objValue = arrSQLTerms[k]._objValue;
					
					String connector = strarrOperators[k-1];
					
					if (connector.equalsIgnoreCase("and")) {
						
						if (add) {
							
							if (strOperator.equals("=")) {
								Object x = tup.getColKey(strColName);
								if (equalsP(x, (objValue)))
									add = true;
								else {
									add = false;
								}
							}
							else if (strOperator.equals(">")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) > 0)
									add = true;
								else {
									add = false;
								}
							}
							else if (strOperator.equals(">=")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) >= 0)
									add = true;
								else {
									add = false;
								}
							}
							else if (strOperator.equals("<")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) < 0)
									add = true;
								else {
									add = false;
								}
							}
							else if (strOperator.equals("<=")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) <= 0)
									add = true;
								else {
									add = false;
								}
							}
							else {
								Object x = tup.getColKey(strColName);
								if (!(equalsP(x, (objValue))))
									add = true;
								else {
									add = false;
								}
							}
						}
					}
					
					else if (connector.equalsIgnoreCase("or")) {
							
						if (strOperator.equals("=")) {
							Object x = tup.getColKey(strColName);
							if (equalsP(x, (objValue)))
								add = true;
						}
						else if (strOperator.equals(">")) {
							Object x = tup.getColKey(strColName);
							if (compareAll(t.getColType(strColName), x, objValue) > 0)
								add = true;
						}
						else if (strOperator.equals(">=")) {
							Object x = tup.getColKey(strColName);
							if (compareAll(t.getColType(strColName), x, objValue) >= 0)
								add = true;
						}
						else if (strOperator.equals("<")) {
							Object x = tup.getColKey(strColName);
							if (compareAll(t.getColType(strColName), x, objValue) < 0)
								add = true;
						}
						else if (strOperator.equals("<=")) {
							Object x = tup.getColKey(strColName);
							if (compareAll(t.getColType(strColName), x, objValue) <= 0)
								add = true;
						}
						else {
							Object x = tup.getColKey(strColName);
							if (!(equalsP(x, (objValue))))
								add = true;
						}
					}
					
					else if (connector.equalsIgnoreCase("xor")) {
						
						if (add) {
							
							if (strOperator.equals("=")) {
								Object x = tup.getColKey(strColName);
								if (equalsP(x, (objValue))) {
									add = false;
									break;
								}
							}
							else if (strOperator.equals(">")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) > 0) {
									add = false;
									break;
								}
							}
							else if (strOperator.equals(">=")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) >= 0) {
									add = false;
									break;
								}
							}
							else if (strOperator.equals("<")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) < 0) {
									add = false;
									break;
								}
							}
							else if (strOperator.equals("<=")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) <= 0) {
									add = false;
									break;
								}
							}
							else {
								Object x = tup.getColKey(strColName);
								if (!(equalsP(x, (objValue)))) {
									add = false;
									break;
								}
							}
						}
						
						else {
						
							if (strOperator.equals("=")) {
								Object x = tup.getColKey(strColName);
								if (equalsP(x, (objValue)))
									add = true;
							}
							else if (strOperator.equals(">")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) > 0)
									add = true;
							}
							else if (strOperator.equals(">=")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) >= 0)
									add = true;
							}
							else if (strOperator.equals("<")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) < 0)
									add = true;
							}
							else if (strOperator.equals("<=")) {
								Object x = tup.getColKey(strColName);
								if (compareAll(t.getColType(strColName), x, objValue) <= 0)
									add = true;
							}
							else {
								Object x = tup.getColKey(strColName);
								if (!(equalsP(x, (objValue))))
									add = true;
							}
						}
					}
				}
				
				if (add)
					results.add(tup);
			}
		}
		
		return results.iterator();
	}
	
	public Vector<Tuple> selectFrom(Table t, Vector<Tuple> list, String strColName, String strOperator, Object objValue) {	
		
		Vector<Tuple> results = new Vector<Tuple>();
		
		for (int j=0; j<list.size(); j++) {
				
			Tuple tup = list.get(j);
				
			if (strOperator.equals("=")) {
				Object x = tup.getColKey(strColName);
				if (equalsP(x, (objValue)))
					results.add(tup);
			}
			else if (strOperator.equals(">")) {
				Object x = tup.getColKey(strColName);
				if (compareAll(t.getColType(strColName), x, objValue) > 0)
					results.add(tup);
			}
			else if (strOperator.equals(">=")) {
				Object x = tup.getColKey(strColName);
				if (compareAll(t.getColType(strColName), x, objValue) >= 0)
					results.add(tup);
			}
			else if (strOperator.equals("<")) {
				Object x = tup.getColKey(strColName);
				if (compareAll(t.getColType(strColName), x, objValue) < 0)
					results.add(tup);
			}
			else if (strOperator.equals("<=")) {
				Object x = tup.getColKey(strColName);
				if (compareAll(t.getColType(strColName), x, objValue) <= 0)
					results.add(tup);
			}
			else {
				Object x = tup.getColKey(strColName);
				if (!(equalsP(x, (objValue))))
					results.add(tup);
			}	
		}
		
		return results;
	}
	
	//ands all of them together but only first one is indexed
	public Vector<Tuple> selectHelper(Table t, Vector<SQLTerm> sqlTerms) {
		
		Vector<Tuple> results = new Vector<Tuple>();
		
		for (int i=0; i<sqlTerms.size(); i++) {
			
			String tableName = sqlTerms.get(i)._strTableName;
			String columnName = sqlTerms.get(i)._strColumnName;
			String strOperator = sqlTerms.get(i)._strOperator;
			Object objValue = sqlTerms.get(i)._objValue;
			
			
			if (i==0) {
				results = select(tableName, columnName, strOperator, objValue);
			}
			
			else {
				results = selectFrom(t, results, columnName, strOperator, objValue);
			}
		}
		
		return results;
	}
	
	public Vector<Tuple> intersect(Vector<Tuple> v1, Vector<Tuple> v2) {
		
		Vector<Tuple> results = new Vector<Tuple>();
		
		for (int i=0; i<v1.size(); i++) {
			
			Tuple tup1 = v1.get(i);
			Hashtable<String, Object> values1 = tup1.getHtblColNameData();
			Set <String> keys1 = values1.keySet();
			
			for (int j=0; j<v2.size(); j++) {
				
				Boolean add = true;
				
				Tuple tup2 = v2.get(j);
				Hashtable<String, Object> values2 = tup2.getHtblColNameData();
				
				for (String key: keys1) {
					if (!(equalsP(values1.get(key), values2.get(key)))) {
						add = false;
						break;
					}
				}
				
				if (add) {
					results.add(tup1);
					break;
				}
			}
		}
		
		return results;
	}
	
	public Vector<Tuple> xor(Vector<Tuple> v1, Vector<Tuple> v2) {
		
		Vector<Tuple> results = new Vector<Tuple>();
		
		for (int i=0; i<v1.size(); i++) {
				
			Tuple tup1 = v1.get(i);
			Hashtable<String, Object> values1 = tup1.getHtblColNameData();
			Set <String> keys1 = values1.keySet();
			
			Boolean common = false;
				
			for (int j=0; j<v2.size(); j++) {
					
				Tuple tup2 = v2.get(j);
				Hashtable<String, Object> values2 = tup2.getHtblColNameData();
				
				Boolean equal = true;
				
				for (String key: keys1) {
					if (!(equalsP(values1.get(key), values2.get(key)))) {
						equal = false;
						break;
					}
				}
				if (equal) {
					common = true;
					break;
				}
			}
			
			if (!(common)) {
				results.add(tup1);
			}
		}
		
		for (int i=0; i<v2.size(); i++) {
			
			Tuple tup1 = v2.get(i);
			Hashtable<String, Object> values1 = tup1.getHtblColNameData();
			Set <String> keys1 = values1.keySet();
			
			Boolean common = false;
				
			for (int j=0; j<v1.size(); j++) {
					
				Tuple tup2 = v1.get(j);
				Hashtable<String, Object> values2 = tup2.getHtblColNameData();
				
				Boolean equal = true;
				
				for (String key: keys1) {
					if (!(equalsP(values1.get(key), values2.get(key)))) {
						equal = false;
						break;
					}
				}
				if (equal) {
					common = true;
					break;
				}
			}
			
			if (!(common)) {
				results.add(tup1);
			}
		}
		
		return results;
	}
	
	public void view (String tableName) {
		
		Table t = (Table) deserialize("data/" + tableName);
		t.viewTable();
	}
	
	public int getArea(Polygon p) {
		
		Dimension dim1 = p.getBounds().getSize();
		return dim1.width * dim1.height;
	}
	
	public int compareAll (String columnType, Object o1, Object o2) {
		
		if (columnType.equals("Integer")) {
			if ((int)o1 > (int)o2) {
				return 1;
			}
			else if ((int)o1 < (int)o2) {
				return -1;
			}
			return 0;
		}
		else if (columnType.equals("Double")) {
			if ((Double)o1 > (Double)o2) {
				return 1;
			}
			else if ((Double)o1 < (Double)o2) {
				return -1;
			}
			return 0;
		}
		else if (columnType.equals("String")) {
			return ((String) o1).compareTo((String) o2);
		}
		else if (columnType.equals("Date")) {
			return ((Date) o1).compareTo((Date) o2);
		}
		else if (columnType.equals("Polygon")) {
			int p1 = getArea((Polygon) o1);
			int p2 = getArea((Polygon) o2);
			
			if (p1 > p2) {
				return 1;
			}
			else if (p1 < p2) {
				return -1;
			}
			return 0;
		}
		else if (columnType.equals("Boolean")) {
			boolean b1 = (boolean) o1;
			boolean b2 = (boolean) o2;
			if (b1) {
				if (b2)
					return 0;
				else
					return 1;
			}
			else {
				if (b2)
					return -1;
				else
					return 0;
			}
		}
		return 0;
	}
	
	public boolean equalsP (Object o1, Object o2) {
		
		if (o1 instanceof Polygon || o1 instanceof PolygonC)
			return Arrays.equals(((Polygon) o1).xpoints, ((Polygon) o2).xpoints) && Arrays.equals(((Polygon) o1).ypoints, ((Polygon) o2).ypoints);
		else
			return o1.equals(o2);	
	}
	
	public void printIndex(String strTableName) {
		
		Table t = (Table) deserialize("data/" + strTableName);
		
		Hashtable<String, String> htblColBIndex = t.getHtblColBIndex();
		Hashtable<String, String> htblColRIndex = t.getHtblColRIndex();
		
		Set <String> keysIndex1 = htblColBIndex.keySet();
		
		for (String key: keysIndex1) {
			System.out.println("BPIndex for column: " + key);
			BTree indexB = (BTree) deserialize(htblColBIndex.get(key));
			indexB.printTree();
			System.out.println();
			System.out.println();
		}
			
		Set <String> keysIndex2 = htblColRIndex.keySet();
			
		for (String key: keysIndex2) {
			System.out.println("RIndex for column: " + key);
			RTree indexR = (RTree) deserialize(htblColRIndex.get(key));
			indexR.printTree();
			System.out.println();
			System.out.println();
		}
		
	}
	
}
