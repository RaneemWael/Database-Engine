package Haramy;

import java.awt.Polygon;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Vector;

@SuppressWarnings("serial")
public class BTree implements Serializable {
	
	int n;
	BNode root;
	String tableName;
	String columnName;
	String columnType;
	String treePath;
	
	public BTree (int n, String tableName, String columnName, String columnType) {
		this.n = n;
		this.tableName = tableName;
		this.columnName = columnName;
		this.columnType = columnType;
		treePath = "data/BP_" + tableName + "_" + columnName;
		serialize (treePath, this);
	}
	
	public String getTreePath() {
		return treePath;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setRoot(BNode root) {
		this.root = root;
	}

	public int getN() {
		return n;
	}

	public BNode getRoot() {
		return root;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}
	
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
	
	public int compareAll (Object o1, Object o2) {
		
		if (this.columnType.equals("Integer")) {
			if ((int)o1 > (int)o2) {
				return 1;
			}
			else if ((int)o1 < (int)o2) {
				return -1;
			}
			return 0;
		}
		else if (this.columnType.equals("Double")) {
			if ((Double)o1 > (Double)o2) {
				return 1;
			}
			else if ((Double)o1 < (Double)o2) {
				return -1;
			}
			return 0;
		}
		else if (this.columnType.equals("String")) {
			return ((String) o1).compareTo((String) o2);
		}
		else if (this.columnType.equals("Date")) {
			return ((Date) o1).compareTo((Date) o2);
		}
		else if (this.columnType.equals("Polygon")) {
			PolygonC p1 = new PolygonC ((Polygon) o1);
			PolygonC p2 = new PolygonC ((Polygon) o2);
			return p1.compareTo(p2);
		}
		else if (this.columnType.equals("Boolean")) {
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
	
	public BLeaf findTupleLeaf (Object indexKey) {
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
		
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
		}
		
		//current is the leaf node containing the tuple
		return (BLeaf) current;
	}
	
	public Vector<String> findTuplePageD (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		for (int i=0; i<current.getValues().size(); i++) {
			if (compareAll(current.getValues().get(i), indexKey) == 0) {
				
				if (current.getPointers().get(i) instanceof BOverflowPage) {
					
					BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
					
					for (int j=0; j<p.getPagePaths().size(); j++) 
						results.add(p.getPagePaths().get(j));
					
					while (p.getNextOverflow() != null) {
						
						BOverflowPage pNew = p.getNextOverflow();
						
						for (int j=0; j<pNew.getPagePaths().size(); j++) 
							results.add(pNew.getPagePaths().get(j));
						
						p = pNew;
						
					}
					
					return results;
				}
				
				else {
					results.add((String) current.getPointers().get(i));
					return results;
				}
			}
		}
		
		return null;
	}
	
	
	public int findTuplePageLast (Object indexKey) {
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		for (int i=0; i<current.getValues().size(); i++) {
			if (compareAll(current.getValues().get(i), indexKey) >= 0) {
				
				if (current.getPointers().get(i) instanceof BOverflowPage) {
					BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
					return ((Page) deserialize(p.getLast())).getPageNum();
				}
				
				else 
					return ((Page) deserialize((String) current.getPointers().get(i))).getPageNum();
			}
		}
		if (current.getPointers().lastElement() instanceof BOverflowPage) {
			BOverflowPage p = (BOverflowPage) current.getPointers().lastElement();
			return ((Page) deserialize(p.getLast())).getPageNum();
		}
		else 
			return ((Page) deserialize((String) current.getPointers().lastElement())).getPageNum();
	}
	
	public int findTuplePageFirst (Object indexKey) {
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		for (int i=0; i<current.getValues().size(); i++) {
			if (compareAll(current.getValues().get(i), indexKey) >= 0) {
				
				if (current.getPointers().get(i) instanceof BOverflowPage) {
					BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
					return ((Page) deserialize(p.getPagePaths().get(0))).getPageNum();
				}
				
				else 
					return ((Page) deserialize((String) current.getPointers().get(i))).getPageNum();
			}
		}
		
		return 0;
	}
	
	public BLeaf findInsertLeaf (Object indexKey) {
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
				
		}
		
		//current is the leaf node containing the tuple
		return (BLeaf) current;
	}
	
	public Vector<String> findGreater (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=0; i<current.getValues().size(); i++) {
				
				if (compareAll(current.getValues().get(i), indexKey) > 0) {
					
					if (current.getPointers().get(i) instanceof BOverflowPage) {
						BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							BOverflowPage pNew = p.getNextOverflow();
							
							for (int j=0; j<pNew.getPagePaths().size(); j++) 
								results.add(pNew.getPagePaths().get(j));
							
							p = pNew;
							
						}
					}
					else {
						results.add((String) current.getPointers().get(i));
					}
				}
			}
			
			current = current.getNext();
		}
		
		return results;
	}
	
	public Vector<String> findGreaterE (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=0; i<current.getValues().size(); i++) {
				
				if (compareAll(current.getValues().get(i), indexKey) >= 0) {
					
					if (current.getPointers().get(i) instanceof BOverflowPage) {
						BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							BOverflowPage pNew = p.getNextOverflow();
							
							for (int j=0; j<pNew.getPagePaths().size(); j++) 
								results.add(pNew.getPagePaths().get(j));
							
							p = pNew;
							
						}
					}
					else {
						results.add((String) current.getPointers().get(i));
					}
				}
			}
			
			current = current.getNext();
		}
		
		return results;
	}
	
	public Vector<String> findLess (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=current.getValues().size()-1; i>=0; i--) {
				
				if (compareAll(current.getValues().get(i), indexKey) < 0) {
					
					if (current.getPointers().get(i) instanceof BOverflowPage) {
						BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							BOverflowPage pNew = p.getNextOverflow();
							
							for (int j=0; j<pNew.getPagePaths().size(); j++) 
								results.add(pNew.getPagePaths().get(j));
							
							p = pNew;
							
						}
					}
					else {
						results.add((String) current.getPointers().get(i));
					}
				}
			}
			
			current = current.getPrev();
		}
		
		return results;
	}
	
	public Vector<String> findLessE (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		BNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof BLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (BNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (BNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (BNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=current.getValues().size()-1; i>=0; i--) {
				
				if (compareAll(current.getValues().get(i), indexKey) <= 0) {
					
					if (current.getPointers().get(i) instanceof BOverflowPage) {
						BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							BOverflowPage pNew = p.getNextOverflow();
							
							for (int j=0; j<pNew.getPagePaths().size(); j++) 
								results.add(pNew.getPagePaths().get(j));
							
							p = pNew;
							
						}
					}
					else {
						results.add((String) current.getPointers().get(i));
					}
				}
			}
			
			current = current.getPrev();
		}
		
		return results;
	}
	
	public Vector<String> findNotEqual (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		BNode current = this.root;
		
		
		while (!(current instanceof BLeaf)) {
			
			current = (BNode) current.getPointers().get(0);
		}
		
		
		while (current != null) {
		
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) != 0) {
					
					if (current.getPointers().get(i) instanceof BOverflowPage) {
						BOverflowPage p = (BOverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							BOverflowPage pNew = p.getNextOverflow();
							
							for (int j=0; j<pNew.getPagePaths().size(); j++) 
								results.add(pNew.getPagePaths().get(j));
							
							p = pNew;
							
						}
					}
					
					else {
						results.add((String) current.getPointers().get(i));
					}
				}
			}
			
			current = current.getNext();
		}
		
		return results;
	}
	
	public void insert(Object indexKey, String pagePath) {
		
		if (root == null) {
			root = new BLeaf (this.n, true);
			root.getValues().add(indexKey);
			root.getPointers().add(pagePath);
			return;
		}
		
		else {
			BLeaf leaf = findInsertLeaf(indexKey);
			leaf.insert(indexKey, pagePath, this);
			
			if (leaf.getIsRoot())
				this.setRoot(leaf);
		}
		serialize(this.getTreePath(), this);
		
	}
	
	public void delete (Object indexKey, String pagePath) {
		
		BLeaf contains = findTupleLeaf(indexKey);
	
		contains.delete(indexKey, pagePath, this);
		
		serialize(this.getTreePath(), this);
		
	}
	
	public void update (Object indexKey, String oldPath, String newPath) {
		
		BLeaf contains = findTupleLeaf(indexKey);
		
		for (int i=0; i<contains.getValues().size(); i++) {
			
			if (contains.getValues().get(i) == indexKey) {
				
				if (contains.getPointers().get(i) instanceof BOverflowPage) {
					BOverflowPage p = (BOverflowPage) contains.getPointers().get(i);
					p.delete(oldPath);
					p.insert(newPath, this);
				}
				else {
					contains.getPointers().remove(i);
					contains.getPointers().add(i, newPath);
				}
				break;
			}
		}
		
		serialize(this.getTreePath(), this);
		
		
	}
	
	public void printTree() {
		
		BNode current = this.root;
		
		while (!(current instanceof BLeaf)) {
			current.printLevel();
			System.out.println();
			current = (BNode) current.getPointers().get(0);
		}
		
		current.printLevel();
	}

}
