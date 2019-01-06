package spark;

import java.io.Serializable;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class Cassandra implements Serializable {

	private static final long serialVersionUID = 1L;

	private Session session;
	private Cluster cluster;
	
	public Cassandra(String keyspace) {
		Builder builder = Cluster.builder().addContactPoint("localhost");
		cluster = builder.build();
		session = cluster.connect();
		
		this.createKeyspace(keyspace);
	}

	public void createKeyspace(String keyspace) {
		String query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
				"WITH replication = {'class':'SimpleStrategy','replication_factor':1}";
		session.execute(query);	
	}

	public void query(String statement) {
		session.execute(statement);
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public PreparedStatement prepareStatement(String stmt) {
		return session.prepare(stmt);	
	}

	public void bind(PreparedStatement stmt, Object... args) {
		session.execute(stmt.bind(args));		
	}
}
