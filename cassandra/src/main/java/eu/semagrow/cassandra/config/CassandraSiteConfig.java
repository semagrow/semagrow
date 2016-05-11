package eu.semagrow.cassandra.config;

import eu.semagrow.core.source.SiteConfig;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraSiteConfig implements SiteConfig {

    public static String TYPE = "CASSANDRA";

    private URI endpoint;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() { }

    @Override
    public Resource export(Graph graph) {
        return null;
    }

    @Override
    public void parse(Graph graph, Resource resource) { }

    public URI getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(URI endpoint) {
        this.endpoint = endpoint;
    }
}
