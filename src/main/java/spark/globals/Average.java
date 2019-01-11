package spark.globals;

import java.io.Serializable;

public class Average implements Serializable, Comparable<Average> {

	private static final long serialVersionUID = 1L;

	public float sum = 0f;
	public int count = 0;

	public float average() {
		if(count == 0) {
			return -1;

		} else {
			return sum / count;		
		}
	}

	public String toString() {
		return "" + average();		
	}

	@Override
	public int compareTo(Average o) {
		if(o.count == 0 || this.count == 0) {
			return 0;
		} else {
			float me = sum/count;
			float him = o.sum/o.count;

			if(me == him) {
				return 0;
			} else if(me > him) {
				return 1;
			} else {
				return -1;
			}
		}
	}
}
