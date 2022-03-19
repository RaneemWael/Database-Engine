package Haramy;

import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements java.io.Serializable {
	
	Vector <Tuple> rows;
	int pageNum;
    
	public Page() {
		rows = new Vector<Tuple>(); 
	}

	//setters and getters
	public Vector<Tuple> getRows() {
		return rows;
	}
	
	public int getPageNum() {
		return pageNum;
	}

	public void setPageNum(int pageNum) {
		this.pageNum = pageNum;
	}
	//end of setters and getters
	
	public void viewPage (String clusterKey) {
		for (int i=0; i<rows.size(); i++) {
			rows.get(i).viewTuple();
			System.out.println();
		}
	}

}
