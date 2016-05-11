package eu.semagrow.cassandra.connector;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by antonis on 5/4/2016.
 */
public class CassandraClient {

    private final Logger logger = LoggerFactory.getLogger(CassandraClient.class);

    private String address;
    private int port;
    private String keyspace;

    private Cluster cluster;
    private Session session;

    public void setCredentials(String address, int port, String keyspace) {
        this.address = address;
        this.port = port;
        this.keyspace = keyspace;
    }

    public String getAddress() {
        return this.address;
    }

    public int getPort() {
        return this.port;
    }

    public String getKeyspace() {
        return this.keyspace;
    }

    public void connect() {
        cluster = Cluster.builder().addContactPoint(address).withPort(port).build();
        session = cluster.connect();
        session.execute("USE " + keyspace + ";");
    }

    public Collection<TableMetadata> getTables() {
        return cluster.getMetadata().getKeyspace(keyspace).getTables();
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public Iterable<Row> execute(String query) {
        logger.info("Sending query: {}", query);
        ResultSet results = session.execute(query);
        return results.all();
    }

    public long executeCount(String query) {

        logger.info("Sending query: {}", query);
        List<Row> results = session.execute(query).all();
        logger.info("results: {}", results);

        for (Row row : results) {
            return row.getLong(0);
        }
        return 0;
    }

}
