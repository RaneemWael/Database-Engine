package Haramy;

import java.awt.Dimension;
import java.awt.Polygon;

@SuppressWarnings({ "rawtypes", "serial" })
public class PolygonC extends Polygon implements Comparable {
	
	Polygon p;

	public PolygonC (Polygon p) {
		this.p = p;
	}
	
	public int compareTo(Object o) {
		Dimension dim1 = p.getBounds().getSize();
		int area1 = dim1.width * dim1.height;
		Dimension dim2 = ((Polygon) o).getBounds().getSize();
		int area2 = dim2.width * dim2.height;
		
		if (area1 > area2) 
			return 1;
		else if (area1 < area2)
			return -1;
		return 0;
	}

}
