package spark.g2q2;

public class G2Q2Database {
	private String origin;
	private String dest;
	private float delay;
	
	public G2Q2Database(String origin, String dest, float delay) {
		this.origin = origin;
		this.dest = dest;
		this.delay = delay;
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
