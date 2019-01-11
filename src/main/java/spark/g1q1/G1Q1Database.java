package spark.g1q1;

import java.io.Serializable;

public class G1Q1Database implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String airport;
	private Long flights;

	public G1Q1Database(String airport, Long flights) {
		this.airport = airport;
		this.flights = flights;
	}
	
	public String getAirport() {
		return airport;
	}

	public Long getFlights() {
		return flights;
	}
}
