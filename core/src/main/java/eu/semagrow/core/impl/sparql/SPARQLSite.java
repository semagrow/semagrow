package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.source.Site;
import eu.semagrow.core.source.SourceCapabilities;
import eu.semagrow.core.source.SourceCapabilitiesBase;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;

/**
 * @author Angelos Charalambidis
 */
public class SPARQLSite implements Site {

    private IRI endpointURI;

    public SPARQLSite(IRI uri) { this.endpointURI = uri; }

    public SPARQLSite() { this.endpointURI = null; }

    @Override
    public boolean isLocal() {
        return endpointURI == null;
    }

    @Override
    public boolean isRemote() {
        return !isLocal();
    }

    public IRI getURI() { return endpointURI; }

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
