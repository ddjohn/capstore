package spark.g3q2;

import java.io.Serializable;

public class TomsFlight implements Serializable {
	private static final long serialVersionUID = 1L;

	public String origin;
	public String dest;
	public String depDate;
	public String depTime;
	public String flight;
	public Float delay;
	
	public TomsFlight(String origin, String dest, String depDate, String depTime, String flight, Float delay) {
		this.origin  = origin;
		this.dest    = dest;
		this.depDate = depDate;
		this.depTime = depTime;
		this.delay = delay;
		this.flight = flight;
	}

	public String toString() {
		return "[" + origin + "->" + dest + ": " + depDate + " " + depTime + " " + flight + "]";
	}
}
