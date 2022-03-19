package Haramy;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("serial")
public class BNode implements Serializable {
	
	int n;
	int maxKeys;
	int maxPointers;
	int minKeys;
	int minPointers;
	BNode parent;
	BNode prev;
	BNode next;
	Boolean isRoot;
	Vector<Object> values;
	Vector<Object> pointers;
	//strings for leaves and nodes for nonleaves
	
	public BNode(int n, Boolean isRoot) {
		this.n = n;
		this.isRoot = isRoot;
		this.maxKeys = n;
		this.maxPointers = n + 1;
		values = new Vector<Object>();
		pointers = new Vector<Object>();
	}

	public Vector<Object> getPointers() {
		return pointers;
	}

	public void setPointers(Vector<Object> pointers) {
		this.pointers = pointers;
	}

	public BNode getParent() {
		return parent;
	}

	public void setParent(BNode parent) {
		this.parent = parent;
	}

	public BNode getPrev() {
		return prev;
	}

	public void setPrev(BNode prev) {
		this.prev = prev;
	}

	public BNode getNext() {
		return next;
	}

	public void setNext(BNode next) {
		this.next = next;
	}

	public Boolean getIsRoot() {
		return isRoot;
	}

	public void setIsRoot(Boolean isRoot) {
		if (isRoot) {
			this.setMinKeys(1);
			this.setMinPointers(1);
			this.setParent(null);
			this.setPrev(null);
			this.setNext(null);
		}
		this.isRoot = isRoot;
	}

	public Vector<Object> getValues() {
		return values;
	}

	public void setValues(Vector<Object> values) {
		this.values = values;
	}

	public int getMinKeys() {
		return minKeys;
	}

	public void setMinKeys(int minKeys) {
		this.minKeys = minKeys;
	}

	public int getMinPointers() {
		return minPointers;
	}

	public void setMinPointers(int minPointers) {
		this.minPointers = minPointers;
	}

	public int getMaxKeys() {
		return maxKeys;
	}

	public int getMaxPointers() {
		return maxPointers;
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
	
	public Object lowestValue() {
		
		if (!(this instanceof BLeaf)) {
			BNode next = (BNode) this.pointers.get(0);
			return next.lowestValue();
		}
		
		else
			return this.getValues().get(0);
		
	}
	
	public void printNode() {
		
		for (int i=0; i<this.getValues().size(); i++) {
			
			if (i == this.getValues().size()-1) 
				System.out.print(this.getValues().get(i));
			else
				System.out.print(this.getValues().get(i) + " ");
		}
		
		
		//System.out.print("/ ");
		
	}
	
	public void printLevel() {
		
		if (this.getValues().size() == 0)
			return;
		
		for (int i=0; i<this.getValues().size(); i++) {
			System.out.print(this.getValues().get(i) + " ");
		}
		
		System.out.print("(");
		
		if (this.parent != null) {
			this.parent.printNode();
		}
		
		else
			System.out.print("null");
		
		
		System.out.print(")");
		System.out.print("(");
		System.out.print(this.lowestValue());
		
		System.out.print(")");
		
		System.out.print("/ ");
		
		if (this.next != null) {
			this.next.printLevel();
		}
		
	}

}
