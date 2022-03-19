package Haramy;

import java.awt.Dimension;

import java.awt.Polygon;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

@SuppressWarnings("serial")
public class RTree implements Serializable {
	
	int n;
	RNode root;
	String tableName;
	String columnName;
	String columnType;
	String treePath;
	
	public RTree (int n, String tableName, String columnName, String columnType) {
		this.n = n;
		this.tableName = tableName;
		this.columnName = columnName;
		this.columnType = columnType;
		treePath = "data/R_" + tableName + "_" + columnName;
		serialize (treePath, this);
	}
	
	public String getTreePath() {
		return treePath;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setRoot(RNode root) {
		this.root = root;
	}

	public int getN() {
		return n;
	}

	public RNode getRoot() {
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
	
	public int getArea(Polygon p) {
		
		Dimension dim1 = p.getBounds().getSize();
		return dim1.width * dim1.height;
	}
	
	public int compareAll (Object o1, Object o2) {
		
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
	
	public boolean equalsP (Object o1, Object o2) {
		
		if (o1 instanceof Polygon || o1 instanceof PolygonC)
			return Arrays.equals(((Polygon) o1).xpoints, ((Polygon) o2).xpoints) && Arrays.equals(((Polygon) o1).ypoints, ((Polygon) o2).ypoints);
		else
			return o1.equals(o2);	
	}
	
	public RLeaf findTupleLeaf (Object indexKey) {
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
		
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
		}
		
		//current is the leaf node containing the tuple
		return (RLeaf) current;
	}
	
	public Vector<String> findTuplePageD (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		for (int i=0; i<current.getValues().size(); i++) {
			if (equalsP(current.getValues().get(i), indexKey) == false) {
				
				if (current.getPointers().get(i) instanceof ROverflowPage) {
					ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
					
					for (int j=0; j<p.getPolygons().size(); j++) {
						if (p.getPolygons().get(i).equals(indexKey))
							results.add(p.getPagePaths().get(j));
					}
					
					while (p.getNextOverflow() != null) {
						
						ROverflowPage pNew = p.getNextOverflow();
						
						for (int j=0; j<p.getPolygons().size(); j++) {
							if (p.getPolygons().get(i).equals(indexKey))
								results.add(p.getPagePaths().get(j));
						}
						
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
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		for (int i=0; i<current.getValues().size(); i++) {
			if (compareAll(current.getValues().get(i), indexKey) >= 0) {
				
				if (current.getPointers().get(i) instanceof ROverflowPage) {
					ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
					return ((Page) deserialize(p.getLast())).getPageNum();
				}
				
				else 
					return ((Page) deserialize((String) current.getPointers().get(i))).getPageNum();
			}
		}
		if (current.getPointers().lastElement() instanceof ROverflowPage) {
			ROverflowPage p = (ROverflowPage) current.getPointers().lastElement();
			return ((Page) deserialize(p.getLast())).getPageNum();
		}
		else 
			return ((Page) deserialize((String) current.getPointers().lastElement())).getPageNum();
	}
	
	public int findTuplePageFirst (Object indexKey) {
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		for (int i=0; i<current.getValues().size(); i++) {
			if (compareAll(current.getValues().get(i), indexKey) >= 0) {
				
				if (current.getPointers().get(i) instanceof ROverflowPage) {
					ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
					return ((Page) deserialize(p.getPagePaths().get(0))).getPageNum();
				}
				
				else 
					return ((Page) deserialize((String) current.getPointers().get(i))).getPageNum();
			}
		}
		
		return 0;
	}
	
	public RLeaf findInsertLeaf (Object indexKey) {
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
				
		}
		
		//current is the leaf node containing the tuple
		return (RLeaf) current;
	}
	
	public Vector<String> findGreater (Object indexKey) {
		
		Vector<String> results = new Vector<String>();
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=0; i<current.getValues().size(); i++) {
				
				if (compareAll(current.getValues().get(i), indexKey) > 0) {
					
					if (current.getPointers().get(i) instanceof ROverflowPage) {
						ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							ROverflowPage pNew = p.getNextOverflow();
							
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
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=0; i<current.getValues().size(); i++) {
				
				if (compareAll(current.getValues().get(i), indexKey) >= 0) {
					
					if (current.getPointers().get(i) instanceof ROverflowPage) {
						ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							ROverflowPage pNew = p.getNextOverflow();
							
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
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=current.getValues().size()-1; i>=0; i--) {
				
				if (compareAll(current.getValues().get(i), indexKey) < 0) {
					
					if (current.getPointers().get(i) instanceof ROverflowPage) {
						ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							ROverflowPage pNew = p.getNextOverflow();
							
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
		
		RNode current = this.root;
		
		boolean found = false;
		
		while (!(current instanceof RLeaf)) {
			
			found = false;
			
			//if it exists in internal nodes
			for (int i=0; i<current.getValues().size(); i++) {
				if (compareAll(current.getValues().get(i), indexKey) == 0) {
					i++;
					current = (RNode) current.getPointers().get(i);
					found = true;
					break;
				}
			}
				
			if (!found) {
				
				//if it is within current node values
				for (int i=0; i<current.getValues().size(); i++) {
					if (compareAll(current.getValues().get(i), indexKey) > 0) {
						current = (RNode) current.getPointers().get(i);
						found = true;
						break;
					}
				}
				
			}
				
			if (!found) {
				
				//if it is greater than greatest key in current node
				current = (RNode) current.getPointers().lastElement();
				found = true;
				
			}
			
		}
		
		//now current is the leaf containing the tuple
		
		while (current != null) {
		
			for (int i=current.getValues().size()-1; i>=0; i--) {
				
				if (compareAll(current.getValues().get(i), indexKey) <= 0) {
					
					if (current.getPointers().get(i) instanceof ROverflowPage) {
						ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							ROverflowPage pNew = p.getNextOverflow();
							
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
		
		RNode current = this.root;
		
		
		while (!(current instanceof RLeaf)) {
			
			current = (RNode) current.getPointers().get(0);
		}
		
		
		while (current != null) {
		
			for (int i=0; i<current.getValues().size(); i++) {
				if (equalsP(current.getValues().get(i), indexKey) == false) {
					
					if (current.getPointers().get(i) instanceof ROverflowPage) {
						ROverflowPage p = (ROverflowPage) current.getPointers().get(i);
						
						for (int j=0; j<p.getPagePaths().size(); j++) 
							results.add(p.getPagePaths().get(j));
						
						while (p.getNextOverflow() != null) {
							
							ROverflowPage pNew = p.getNextOverflow();
							
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
			root = new RLeaf (this.n, true);
			root.getValues().add(indexKey);
			root.getPointers().add(pagePath);
			return;
		}
		
		else {
			RLeaf leaf = findInsertLeaf(indexKey);
			leaf.insert(indexKey, pagePath, this);
			
			if (leaf.getIsRoot())
				this.setRoot(leaf);
		}
		serialize(this.getTreePath(), this);
		
	}
	
	public void delete (Object indexKey, String pagePath) {
		
		RLeaf contains = findTupleLeaf(indexKey);
	
		contains.delete(indexKey, pagePath, this);
		
		serialize(this.getTreePath(), this);
		
	}
	
	public void update (Object indexKey, String oldPath, String newPath) {
		
		RLeaf contains = findTupleLeaf(indexKey);
		
		for (int i=0; i<contains.getValues().size(); i++) {
			
			if (compareAll(contains.getValues().get(i), indexKey) == 0) {
				
				if (contains.getPointers().get(i) instanceof ROverflowPage) {
					ROverflowPage p = (ROverflowPage) contains.getPointers().get(i);
					p.delete(indexKey, oldPath);
					p.insert(indexKey, newPath, this);
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
		
		RNode current = this.root;
		
		while (!(current instanceof RLeaf)) {
			current.printLevel();
			System.out.println();
			current = (RNode) current.getPointers().get(0);
		}
		
		current.printLevel();
	}

}
