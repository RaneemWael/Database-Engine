package Haramy;

import java.awt.Polygon;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

@SuppressWarnings("serial")
public class ROverflowPage implements Serializable {
	
	Vector<Polygon> polygons;
	Vector<String> pagePaths;
	int max;
	ROverflowPage nextOverflow;
	ROverflowPage prevOverflow;
	
	public ROverflowPage (int max) {
		this.max = max;
		nextOverflow = null;
		prevOverflow = null;
		polygons = new Vector<Polygon>();
		pagePaths = new Vector<String>();
	}
	
	public Vector<Polygon> getPolygons() {
		return polygons;
	}

	public void setPolygons(Vector<Polygon> polygons) {
		this.polygons = polygons;
	}

	public ROverflowPage getNextOverflow() {
		return nextOverflow;
	}

	public void setNextOverflow(ROverflowPage nextOverflow) {
		this.nextOverflow = nextOverflow;
	}

	public ROverflowPage getPrevOverflow() {
		return prevOverflow;
	}

	public void setPrevOverflow(ROverflowPage prevOverflow) {
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

	public void insert (Object poly, String pagePath, RTree t) {
		
		if (this.getPagePaths().size() < this.max) {
			this.getPolygons().add((Polygon) poly);
			this.getPagePaths().add(pagePath);
		}
		
		else {
			if (this.getNextOverflow() == null) {
				ROverflowPage newOverflow = new ROverflowPage (this.max);
				this.setNextOverflow(newOverflow);
				newOverflow.setPrevOverflow(this);
				newOverflow.insert(poly, pagePath, t);
			}
			else {
				ROverflowPage nextOverflow = this.getNextOverflow();
				nextOverflow.insert(poly, pagePath, t);
			}
		}
	}
	
	public String delete (Object poly, String pagePath) {
		
		for (int i=0; i<this.getPolygons().size(); i++) {
			
			Polygon p = this.getPolygons().get(i);
			
			if (equalsP((Polygon) poly, p) && (this.getPagePaths().get(i)).equals(pagePath)) {
				this.getPolygons().remove(i);
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
		
		ROverflowPage nextOverflow = this.getNextOverflow();
		nextOverflow.delete(poly, pagePath);
		
		return null;
		
	}
	
	public String getLast() {
		
		if (this.getNextOverflow() != null) 
			return (this.getNextOverflow()).getLast();
		
		else 
			return this.getPagePaths().lastElement();
		
	}
	
	public boolean equalsP (Object o1, Object o2) {
		
		if (o1 instanceof Polygon || o1 instanceof PolygonC)
			return Arrays.equals(((Polygon) o1).xpoints, ((Polygon) o2).xpoints) && Arrays.equals(((Polygon) o1).ypoints, ((Polygon) o2).ypoints);
		else
			return o1.equals(o2);	
	}
	


}
