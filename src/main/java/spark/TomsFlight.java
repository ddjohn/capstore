package spark;

import java.io.Serializable;

public class TomsFlight implements Serializable {
	private static final long serialVersionUID = 1L;

	public String origin;
	public String dest;
	public String depDate;
	public String depTime;
	
	public TomsFlight(String origin, String dest, String depDate, String depTime) {
		this.origin  = origin;
		this.dest    = dest;
		this.depDate = depDate;
		this.depTime = depTime;
	}

	public String toString() {
		return "origin:" + origin + " dest:" + dest + " departure: " + depDate + " " + depTime;
	}
}
