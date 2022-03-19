package Haramy;

import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("serial")
public class BOverflowPage implements Serializable {
	
	Vector<String> pagePaths;
	int max;
	BOverflowPage nextOverflow;
	BOverflowPage prevOverflow;
	
	public BOverflowPage (int max) {
		this.max = max;
		nextOverflow = null;
		prevOverflow = null;
		pagePaths = new Vector<String>();
	}
	
	public BOverflowPage getNextOverflow() {
		return nextOverflow;
	}

	public void setNextOverflow(BOverflowPage nextOverflow) {
		this.nextOverflow = nextOverflow;
	}

	public BOverflowPage getPrevOverflow() {
		return prevOverflow;
	}

	public void setPrevOverflow(BOverflowPage prevOverflow) {
		this.prevOverflow = prevOverflow;
	}

	public Vector<String> getPagePaths() {
		return pagePaths;
	}

	public void setPagePaths(Vector<String> pagePaths) {
		this.pagePaths = pagePaths;
	}

	public int getMax() {
		return max;
	}

	public void insert (String pagePath, BTree t) {
		
		if (this.getPagePaths().size() < this.max)
			this.getPagePaths().add(pagePath);
		
		else {
			if (this.getNextOverflow() == null) {
				BOverflowPage newOverflow = new BOverflowPage (this.max);
				this.setNextOverflow(newOverflow);
				newOverflow.setPrevOverflow(this);
				newOverflow.insert(pagePath, t);
			}
			else {
				BOverflowPage nextOverflow = this.getNextOverflow();
				nextOverflow.insert(pagePath, t);
			}
		}
	}
	
	public String delete (String pagePath) {
		
		for (int i=0; i<this.getPagePaths().size(); i++) {
			if ((this.getPagePaths().get(i)).equals(pagePath)) {
				this.getPagePaths().remove(i);
				if (this.getPagePaths().size() == 1 && this.getPrevOverflow()==null) {
					String pathLeft = this.getPagePaths().remove(0);
					return pathLeft;
				}
				else if (this.getPagePaths().size() == 0) {
					this.getPrevOverflow().setNextOverflow(this.getNextOverflow());
					this.getNextOverflow().setPrevOverflow(this.getPrevOverflow());
				}
				return null;
			}
		}
		
		BOverflowPage nextOverflow = this.getNextOverflow();
		nextOverflow.delete(pagePath);
		
		return null;
		
	}
	
	public String getLast() {
		
		if (this.getNextOverflow() != null) 
			return (this.getNextOverflow()).getLast();
		
		else 
			return this.getPagePaths().lastElement();
		
	}


}
