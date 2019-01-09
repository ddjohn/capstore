package spark;

import java.io.Serializable;

public class TomsFlight implements Serializable {
	private static final long serialVersionUID = 1L;

	public String origin;
	public String middle;
	public String dest;
	public String originDate;
	public String middleDate;
	
	public TomsFlight(String origin, String middle, String dest, String originDate, String middleDate) {
		this.origin = origin;
		this.middle = middle;
		this.dest   = dest;
		this.originDate   = originDate;
		this.middleDate   = middleDate;
	}

	public String toString() {
		return "origin=" + origin + ",middle=" + middle + ",dest=" + dest;
	}
}
