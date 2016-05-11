package eu.semagrow.cassandra;

import eu.semagrow.cassandra.connector.CassandraSchema;
import eu.semagrow.cassandra.connector.CassandraSchemaInit;
import eu.semagrow.core.source.Site;
import eu.semagrow.core.source.SourceCapabilities;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraSite implements Site {

    static final String TYPE = "CASSANDRA";

    private final URI endpoint;

    public CassandraSite(URI endpoint) { this.endpoint = endpoint; }

    public Resource getID() { return getURI(); }

    public String getType() { return TYPE; }

    public URI getURI() { return endpoint; }

    @Override
    public boolean isLocal() { return false; }

    @Override
    public boolean isRemote() { return true; }

    @Override
    public SourceCapabilities getCapabilities() {
        return new CassandraCapabilities(getCassandraSchema(), getBase());
    }

    public CassandraSchema getCassandraSchema() {
        return CassandraSchemaInit.getInstance().getCassandraSchema(endpoint);
    }

    public String getBase() {
        return getCassandraSchema().getBase();
    }

    public String getAddress() {
        return getCassandraSchema().getAddress();
    }

    public int getPort() {
        return getCassandraSchema().getPort();
    }

    public String getKeyspace() {
        return getCassandraSchema().getKeyspace();
    }

    public String toString() {
        return endpoint.toString();
    }

}
