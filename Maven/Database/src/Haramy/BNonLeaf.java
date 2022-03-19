package Haramy;

@SuppressWarnings("serial")
public class BNonLeaf extends BNode {
	
	public BNonLeaf(int n, Boolean isRoot) {
		super (n, isRoot);
		if (isRoot) {
			this.minKeys = 1;
			this.minPointers = 1;
			this.parent = null;
			this.next = null;
			this.prev = null;
		}
		else {
			this.minKeys = (int) Math.ceil((n+1)/2.0) - 1;
			this.minPointers = (int) Math.ceil((n+1)/2.0);
		}
	}
	
	public void replace (Object indexKey1, Object indexKey2, BTree t) {
		
		if (this.isRoot) {
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey1) == 0) {
					this.getValues().remove(i);
					this.getValues().add(i, indexKey2);
					return;
				}
			}
			return;
		}
		
		else {
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey1) == 0) {
					this.getValues().remove(i);
					this.getValues().add(i, indexKey2);
					return;
				}
			}
			
			BNonLeaf nextNode = (BNonLeaf) (this.getParent());
			nextNode.replace(indexKey1, indexKey2, t);	
		}
	}
	
	public void insert (Object indexKey, BNode nodePath, BTree t) {
		
		// 1) space available in node
		if (this.getValues().size() < this.maxKeys) {
			Boolean inserted = false;
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey) > 0) {
					this.getValues().add(i, indexKey);
					this.getPointers().add(i, nodePath);
					inserted = true;
					break;
				}
			}
			
			if (inserted == false) {
				this.getValues().add(indexKey);
				this.getPointers().add(this.getValues().size()-1, nodePath);
				inserted = true;
			}
			
			if (this.isRoot) {
				t.setRoot(this);
			}
		}
				
		// 3) non-leaf overflow (add new one and shift if necessary)
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
						this.getPointers().add(i, nodePath);
						inserted = true;
						break;
					}
				}
				if (inserted == false) {
					this.getValues().add(indexKey);
					this.getPointers().add(this.getValues().size()-1, nodePath);
					inserted = true;
				}
				
				//create new node and add extras			
				BNonLeaf newNode = new BNonLeaf (t.getN(), false);
				newNode.setNext(this);
				this.setPrev(newNode);
				
				BNode fixParent = (BNode) (this.getPointers().get(0));
				fixParent.setParent(newNode);
				newNode.getPointers().add(this.getPointers().remove(0));
				
				int j = 0;
				while (j < this.minKeys) {
					newNode.getValues().add(this.getValues().remove(0));
					fixParent = (BNode) (this.getPointers().get(0));
					fixParent.setParent(newNode);
					newNode.getPointers().add(this.getPointers().remove(0));
					j++;
				}
				
				Object indexRoot = this.getValues().remove(0);
				
				// B -- create new root and add pointers to point to the 2 nodes
				BNonLeaf newRoot = new BNonLeaf (t.getN(), true);
				newRoot.getValues().add(indexRoot);
				newRoot.getPointers().add(newNode);
				newRoot.getPointers().add(this);
				
				this.setParent(newRoot);
				newNode.setParent(newRoot);
				
				//update tree root
				t.setRoot(newRoot);
				
				
			}
					
			else {
				
				BNonLeaf parentNode = (BNonLeaf) (this.getParent());
				
				//insert in right position
				Boolean inserted = false;
				for (int i=0; i<this.getValues().size(); i++) {
					if (t.compareAll(values.get(i), indexKey) > 0) {
						this.getValues().add(i, indexKey);
						this.getPointers().add(i, nodePath);
						inserted = true;
						break;
					}
				}
				if (inserted == false) {
					this.getValues().add(indexKey);
					this.getPointers().add(this.getValues().size()-1, nodePath);
					inserted = true;
				}
				
				//create new node and add extras			
				BNonLeaf newNode = new BNonLeaf (t.getN(), false);
				newNode.setNext(this);
				if (this.prev != null) {
					newNode.setPrev(this.getPrev());
					BNonLeaf prevLeaf = (BNonLeaf) (this.getPrev());
					prevLeaf.setNext(newNode);
				}
				this.setPrev(newNode);
				
				BNode fixParent = (BNode) (this.getPointers().get(0));
				fixParent.setParent(newNode);
				newNode.getPointers().add(this.getPointers().remove(0));
				
				int j = 0;
				while (j < this.minKeys) {
					newNode.getValues().add(this.getValues().remove(0));
					fixParent = (BNode) (this.getPointers().get(0));
					fixParent.setParent(newNode);
					newNode.getPointers().add(this.getPointers().remove(0));
					j++;
				}
				
				//fix parent node
				newNode.setParent(this.getParent());
				Object insertParent = this.getValues().remove(0);
				BNonLeaf pointerNode = newNode;
				//pointerNode at i-1 in pointers, insertParent at i in values
				parentNode.insert(insertParent, pointerNode, t);			
			}
		}
	}
	
	public void delete (Object indexKey, boolean isNegative, BTree t) {
		//delete value and pointer at i+1 or i-1 ala hasab isNegative
		
		if (this.isRoot) {
			
			for (int i=0; i<this.getValues().size(); i++) {
				if (t.compareAll(values.get(i), indexKey) == 0) {
					this.getValues().remove(i);
					if (isNegative)
						this.getPointers().remove(i);
					else
						this.getPointers().remove(i+1);
					if (this.getPointers().size() == 1) {
						BNode newRoot = (BNode) (this.getPointers().get(0));
						newRoot.setIsRoot(true);
						t.setRoot(newRoot);
					}
					return;
				}
			}
		}
		
		else {
			
			// 1) node can handle deletion
			if (this.getValues().size()-1 >= this.minKeys) {
				
				for (int i=0; i<this.getValues().size(); i++) {
					if (t.compareAll(values.get(i), indexKey) == 0) {
						this.getValues().remove(i);
						if (isNegative)
							this.getPointers().remove(i);
						else
							this.getPointers().remove(i+1);
						return;
					}
				}			
			}
			
			// 2) node can't handle deletion
			else {
				
				BNonLeaf nextNode = null;
				BNonLeaf prevNode = null;
				
				if (this.getNext() != null)
					nextNode = (BNonLeaf) (this.getNext());
				
				if (this.getPrev() != null)
					prevNode = (BNonLeaf) (this.getPrev());
				
				//borrow right
				if ((nextNode != null) && (nextNode.getParent()).equals(this.getParent()) && nextNode.getValues().size()-1 >= nextNode.getMinKeys()) {
				
					for (int i=0; i<this.getValues().size(); i++) {
						if (t.compareAll(values.get(i), indexKey) == 0) {
							this.getValues().remove(i);
							if (isNegative)
								this.getPointers().remove(i);
							else
								this.getPointers().remove(i+1);
							
							break;
						}
					}
					
					BNode borrowedNode = (BNode) nextNode.getPointers().get(0);
					borrowedNode.setParent(this);
					Object borrowedValue = borrowedNode.lowestValue();
					
					this.getValues().add(borrowedValue);
					this.getPointers().add(borrowedNode);
					
					Object lowValueAfterRemove = ((BNode) nextNode.getPointers().get(1)).lowestValue();
					
					BNonLeaf nextParent = (BNonLeaf) (nextNode.getParent());
					nextParent.replace(borrowedValue, lowValueAfterRemove, t);
					nextNode.delete(lowValueAfterRemove, true, t);
					
					return;
				}
				
				//borrow left
				else if ((prevNode != null) && (prevNode.getParent()).equals(this.getParent()) && prevNode.getValues().size()-1 >= prevNode.getMinKeys()) {
					
					for (int i=0; i<this.getValues().size(); i++) {
						if (t.compareAll(values.get(i), indexKey) == 0) {
							this.getValues().remove(i);
							if (isNegative)
								this.getPointers().remove(i);
							else
								this.getPointers().remove(i+1);
						}
					}
					
					Object oldFirst = this.lowestValue();
				
					BNode borrowedNode = (BNode) prevNode.getPointers().get(prevNode.getPointers().size()-1);
					borrowedNode.setParent(this);
					
					this.getPointers().add(0, borrowedNode);
					this.getValues().add(0, oldFirst);
					Object newFirst = this.lowestValue();
					
					BNonLeaf parentNode = (BNonLeaf) (this.getParent());
					parentNode.replace(oldFirst, newFirst, t);
					
					prevNode.delete(newFirst, false, t);
					
					return;
				}
				
				
				//merge
				else {
					
					//merge with left
					if ((prevNode != null) && (prevNode.getParent()).equals(this.getParent()) && (prevNode.getValues().size() + (this.getValues().size()-1)) <= prevNode.getMaxKeys()) {
			
						for (int i=0; i<this.getValues().size(); i++) {
							if (t.compareAll(values.get(i), indexKey) == 0) {
								this.getValues().remove(i);
								if (isNegative)
									this.getPointers().remove(i);
								else
									this.getPointers().remove(i+1);
							}
						}
						
						Object oldFirst = this.lowestValue();
						prevNode.getValues().add(oldFirst);
												
						for (int i=0; i<this.getValues().size(); i++) {
							BNode move = (BNode) (this.getPointers().get(0));
							move.setParent(prevNode);
							prevNode.getPointers().add(this.getPointers().remove(0));
							prevNode.getValues().add(this.getValues().remove(0));
						}
						
						BNode move = (BNode) (this.getPointers().get(0));
						move.setParent(prevNode);
						prevNode.getPointers().add(this.getPointers().remove(0));
						
						prevNode.setNext(this.getNext());
						
						if (this.getNext() != null) {
							nextNode.setPrev(this.getPrev());
						}
						
						BNonLeaf parentNode = (BNonLeaf) (this.getParent());
						parentNode.delete(oldFirst, false, t);
						return;
					}
					
					//merge with right
					else {
						
						for (int i=0; i<this.getValues().size(); i++) {
							if (t.compareAll(values.get(i), indexKey) == 0) {
								this.getValues().remove(i);
								if (isNegative)
									this.getPointers().remove(i);
								else
									this.getPointers().remove(i+1);
							}
						}
						
						
						Object mergeFirst = nextNode.lowestValue();
						nextNode.getValues().add(0, mergeFirst);
						
						BNonLeaf nextParent = (BNonLeaf) (nextNode.getParent());
						nextParent.delete(mergeFirst, true, t);
						
						for (int i=this.getValues().size()-1; i>=0; i--) {
							BNode move = (BNode) (this.getPointers().get(i));
							move.setParent(nextNode);
							nextNode.getPointers().add(0, this.getPointers().get(i));
							nextNode.getValues().add(0, this.getValues().get(i));
						}
						
						BNode move = (BNode) (this.getPointers().get(0));
						move.setParent(nextNode);
						nextNode.getPointers().add(0, this.getPointers().get(0));
						nextNode.setPrev(this.getPrev());
						
						if (this.getPrev() != null) {
							prevNode.setNext(this.getNext());
						}
						
						return;
					}	
				}	
			}
		}
	}
	

}
