package spark.g1q3;

public class G1Q3Database {
	private String day;
	private float delay;
	
	public G1Q3Database(String day, float delay) {
		this.day = day;
		this.delay = delay;
	}

	public String getDay() {
		return day;
	}

	public float getDelay() {
		return delay;
	}
}
