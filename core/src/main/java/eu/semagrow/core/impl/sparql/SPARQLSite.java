package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.source.Site;
import eu.semagrow.core.source.SourceCapabilities;
import eu.semagrow.core.source.SourceCapabilitiesBase;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

/**
 * @author Angelos Charalambidis
 */
public class SPARQLSite implements Site {

    private URI endpointURI;

    public SPARQLSite(URI uri) { this.endpointURI = uri; }

    public SPARQLSite() { this.endpointURI = null; }

    @Override
    public boolean isLocal() {
        return endpointURI == null;
    }

    @Override
    public boolean isRemote() {
        return !isLocal();
    }

    public URI getURI() { return endpointURI; }

    public Resource getID() { return getURI(); }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SPARQLSite) {
            SPARQLSite s = (SPARQLSite)o;
            return (s.isLocal() && this.isLocal()) ||
                    (s.getURI() != null && s.getURI().equals(this.getURI()));
        }
        return false;
    }

    public String getType() { return "SPARQL"; }

    @Override
    public SourceCapabilities getCapabilities() {

        /*
        // FIXME
        if (endpointURI != null && endpointURI.stringValue().equals("http://my.cassandra.antru/"))
            return new CassandraCapabilities();
        */

        return new SourceCapabilitiesBase();
    }

    //public String getType() { return "SPARQL"; }
}
