package spark.g1q2;

public class G1Q2Database {
	private String carrier;
	private float delay;
	
	public G1Q2Database(String carrier, float delay) {
		this.carrier = carrier;
		this.delay = delay;
	}

	public String getCarrier() {
		return carrier;
	}

	public float getDelay() {
		return delay;
	}
}
