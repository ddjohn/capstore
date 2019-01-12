package spark.g2q3;

public class G2Q3Database {
	private String carrier;
	private String origin;
	private String dest;
	private float delay;
	
	public G2Q3Database(String origin, String dest, String carrier, float delay) {
		this.carrier = carrier;
		this.origin = origin;
		this.dest = dest;
		this.delay = delay;
	}

	public String getCarrier() {
		return carrier;
	}

	public String getOrigin() {
		return origin;
	}

	public String getDest() {
		return dest;
	}

	public float getDelay() {
		return delay;
	}
}
