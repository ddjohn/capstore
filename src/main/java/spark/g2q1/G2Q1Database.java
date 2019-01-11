package spark.g2q1;

public class G2Q1Database {
	private String origin;
	private String carrier;
	private float delay;
	
	public G2Q1Database(String origin, String carrier, float delay) {
		this.origin = origin;
		this.carrier = carrier;
		this.delay = delay;
	}

	public String getOrigin() {
		return origin;
	}

	public String getCarrier() {
		return carrier;
	}

	public float getDelay() {
		return delay;
	}
}
