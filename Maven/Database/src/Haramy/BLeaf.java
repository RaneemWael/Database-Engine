package Haramy;

@SuppressWarnings("serial")
public class BLeaf extends BNode {

	public BLeaf (int n, Boolean isRoot) {
		super (n, isRoot);
		if (isRoot) {
			this.minKeys = 1;
			this.minPointers = 1;
			this.parent = null;
			this.next = null;
			this.prev = null;
		}
		else {
			this.minKeys = (n+1)/2;
			this.minPointers = ((n+1)/2) - 1;
			this.maxPointers = n;
			//-1 in both because the pointer to the next node counts even if it is empty
		}
	}
	
	public void insert(Object indexKey, String pagePath, BTree t) {
		
		//check if it's overflow
		for (int i=0; i<this.getValues().size(); i++) {
			if (t.compareAll(values.get(i), indexKey) == 0) {
				if (this.getPointers().get(i) instanceof BOverflowPage) {
					BOverflowPage overPage = (BOverflowPage) this.getPointers().get(i);
					overPage.insert(pagePath, t);
					return;
				}
				else {
					String oldPagePath = (String) this.getPointers().remove(i);
					BOverflowPage overPage = new BOverflowPage (t.getN());
					this.getPointers().add(i, overPage);
					overPage.insert(oldPagePath, t);
					overPage.insert(pagePath, t);
					return;
				}
			}
			if (this.isRoot)
				t.setRoot(this);
		}		
		
		// 1) space available in leaf
		if (this.getValues().size() < this.maxKeys) {
			
			Boolean inserted = false;
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey) > 0) {
					this.getValues().add(i, indexKey);
					this.getPointers().add(i, pagePath);
					inserted = true;
					break;
				}
			}
			if (inserted == false) {
				this.getValues().add(indexKey);
				this.getPointers().add(pagePath);
				inserted = true;
				return;
			}
		}
		
		// 2) leaf overflow (add new one and shift if necessary)
		else {
			
			// 4) new root
			if (this.isRoot) {
				
				this.setIsRoot(false);
				
				// A -- do normal overflow first
				
				//insert in right position
				Boolean inserted = false;
				for (int i=0; i<this.getValues().size(); i++) {
					if (t.compareAll(values.get(i), indexKey) > 0) {
						this.getValues().add(i, indexKey);
						this.getPointers().add(i, pagePath);
						inserted = true;
						break;
					}
				}
				if (inserted == false) {
					this.getValues().add(indexKey);
					this.getPointers().add(pagePath);
					inserted = true;
				}
				
				//create new leaf and add extras			
				BLeaf newLeaf = new BLeaf (t.getN(), false);
				newLeaf.setNext(this);
				this.setPrev(newLeaf);
				
				int j = 0;
				while (j < this.minKeys) {
					newLeaf.getValues().add(this.getValues().remove(0));
					newLeaf.getPointers().add(this.getPointers().remove(0));
					j++;
				}
				
				Object indexRoot = this.getValues().get(0);
				
				// B -- create new root and add pointers to point to the 2 nodes
				BNonLeaf newRoot = new BNonLeaf (t.getN(), true);
				newRoot.getValues().add(indexRoot);
				newRoot.getPointers().add(newLeaf);
				newRoot.getPointers().add(this);
				
				this.setParent(newRoot);
				newLeaf.setParent(newRoot);
				
				//update tree root
				t.setRoot(newRoot);
			}
			
			else {
				
				BNonLeaf parentNode = (BNonLeaf) this.getParent();
				
				//insert in right position
				Boolean inserted = false;
				for (int i=0; i<this.getValues().size(); i++) {
					if (t.compareAll(values.get(i), indexKey) > 0) {
						this.getValues().add(i, indexKey);
						this.getPointers().add(i, pagePath);
						inserted = true;
						break;
					}
				}
				if (inserted == false) {
					this.getValues().add(indexKey);
					this.getPointers().add(pagePath);
					inserted = true;
				}
				
				//create new leaf and add extras			
				BLeaf newLeaf = new BLeaf (t.getN(), false);
				newLeaf.setNext(this);
				if (this.prev != null) {
					newLeaf.setPrev(this.getPrev());
					BLeaf prevLeaf = (BLeaf) this.getPrev();
					prevLeaf.setNext(newLeaf);
				}
				this.setPrev(newLeaf);
			
				int j = 0;
				while (j < this.minKeys) {
					newLeaf.getValues().add(this.getValues().remove(0));
					newLeaf.getPointers().add(this.getPointers().remove(0));
					j++;
				}
				
				//fix parent node
				newLeaf.setParent(this.getParent());
				Object insertParent = this.getValues().get(0);
				BNode pointerNode = newLeaf;
				//pointerNode at i-1 in pointers, insertParent at i in values
				parentNode.insert(insertParent, pointerNode, t);
			}
		}
	}
	
	public void delete (Object indexKey, String pagePath, BTree t) {
		
		// check if overflow therefore can handle deletion for sure
		for (int i=0; i<this.getValues().size(); i++) {
			if (t.compareAll(values.get(i), indexKey) == 0) {
				if (this.getPointers().get(i) instanceof BOverflowPage) {
					BOverflowPage overPage = (BOverflowPage) this.getPointers().get(i);
					String pageLeft = overPage.delete(pagePath);
					if (pageLeft != null) {
						this.getPointers().remove(i);
						this.getPointers().add(i, pageLeft);
					}
					return;
				}
			}
		}
		
		if (this.isRoot) {
			
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey) == 0) {
					this.getValues().remove(i);
					this.getPointers().remove(i);
					return;
				}
			}
		}
		
		// 1) leaf can handle deletion
		if (this.getValues().size()-1 >= this.minKeys) {
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey) == 0) {
					this.getValues().remove(i);
					this.getPointers().remove(i);
					if (i==0) {
						Object newKey = this.getValues().get(0);
						BNonLeaf parentNode = (BNonLeaf) (this.getParent());
						parentNode.replace(indexKey, newKey, t);
					}
					return;
				}
			}			
		}
		
		// 2) leaf can't handle deletion
		else {
			
			BLeaf nextNode = null;
			BLeaf prevNode = null;
			
			if (this.getNext() != null)
				nextNode = (BLeaf) (this.getNext());
			
			if (this.getPrev() != null)
				prevNode = (BLeaf) (this.getPrev());
			
			//borrow right
			if ((nextNode != null) && (nextNode.getParent()).equals(this.getParent()) && nextNode.getValues().size()-1 >= nextNode.getMinKeys()) {
				for (int i=0; i<this.getValues().size(); i++) {
					if (t.compareAll(values.get(i), indexKey) == 0) {
						this.getValues().remove(i);
						this.getPointers().remove(i);
						Object borrowed = nextNode.getValues().remove(0);
						this.getValues().add(borrowed);
						this.getPointers().add(nextNode.getPointers().remove(0));
						if (i==0) {
							Object newKey = this.getValues().get(0);
							BNonLeaf parentNode = (BNonLeaf) (this.getParent());
							parentNode.replace(indexKey, newKey, t);
						}
						BNonLeaf nextParent = (BNonLeaf) (nextNode.getParent());
						nextParent.replace(borrowed, nextNode.getValues().get(0), t);
						return;
					}
				}			
			}
			
			//borrow left
			else if ((prevNode != null) && (prevNode.getParent()).equals(this.getParent()) && prevNode.getValues().size()-1 >= prevNode.getMinKeys()) {
				
				Object oldFirst = this.getValues().get(0);
				
				for (int i=0; i<this.getValues().size(); i++) {
					if (t.compareAll(values.get(i), indexKey) == 0) {
						this.getValues().remove(i);
						this.getPointers().remove(i);
						break;
					}
				}
				Object borrowed = prevNode.getValues().remove(prevNode.getValues().size()-1);
				this.getValues().add(0, borrowed);
				this.getPointers().add(0, prevNode.getPointers().remove(prevNode.getPointers().size()-1));
				
				BNonLeaf parentNode = (BNonLeaf) (this.getParent());
				parentNode.replace(oldFirst, borrowed, t);
				return;
			}
			
			//merge
			else {
				
				//merge with left
				if ((prevNode != null) && (prevNode.getParent()).equals(this.getParent()) && (prevNode.getValues().size() + (this.getValues().size()-1)) <= prevNode.getMaxKeys()) {
					
					
					Object deletedFirst = this.getValues().get(0);
					
					for (int i=0; i<this.getValues().size(); i++) {
						if (t.compareAll(values.get(i), indexKey) == 0) {
							this.getValues().remove(i);
							this.getPointers().remove(i);
						}
					}
					
					for (int i=0; i<this.getValues().size(); i++) {
						prevNode.getValues().add(this.getValues().get(i));
						prevNode.getPointers().add(this.getPointers().get(i));
					}
					
					prevNode.setNext(this.getNext());
					
					if (this.getNext() != null) {
						nextNode.setPrev(this.getPrev());
					}
					
					BNonLeaf parentNode = (BNonLeaf) (this.getParent());
					parentNode.delete(deletedFirst, false, t);
					return;
				}
				
				//merge with right
				else {
					
					
					Object deletedFirst = this.getValues().get(0);
					Object nextFirst = nextNode.getValues().get(0);
					
					for (int i=0; i<this.getValues().size(); i++) {
						if (t.compareAll(values.get(i), indexKey) == 0) {
							this.getValues().remove(i);
							this.getPointers().remove(i);
						}
					}
					
					for (int i=this.getValues().size()-1; i>=0; i--) {
						nextNode.getValues().add(0, this.getValues().get(i));
						nextNode.getPointers().add(0, this.getPointers().get(i));
					}
					
					nextNode.setPrev(this.getPrev());
					
					if (this.getPrev() != null) {
						prevNode.setNext(this.getNext());
					}
					
					BNonLeaf parentNode = (BNonLeaf) (this.getParent());
					parentNode.replace(deletedFirst, nextNode.getValues().get(0), t);
					parentNode.delete(nextFirst, true, t);
					return;
				}
				
			}
		}
	}

}
