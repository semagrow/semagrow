package org.semagrow.connector.sparql;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;
import org.eclipse.rdf4j.model.Resource;

import java.net.URL;

/**
 * @author Angelos Charalambidis
 */
public class SPARQLSite implements Site {

    private URL endpointURI;

    public SPARQLSite(URL uri) {
        assert uri != null;
        this.endpointURI = uri;
    }

    @Override
    public boolean isRemote() {
        return endpointURI != null;
    }

    public URL getURL() { return endpointURI; }

    public Resource getID() { return SimpleValueFactory.getInstance().createURI(getURL().toString()); }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SPARQLSite) {
            SPARQLSite s = (SPARQLSite)o;
            return (s.getURL().equals(this.getURL()));
        }
        return false;
    }

    public String getType() { return "SPARQL"; }

    @Override
    public SiteCapabilities getCapabilities() {

        /*
        // FIXME
        if (endpointURI != null && endpointURI.stringValue().equals("http://my.cassandra.antru/"))
            return new CassandraCapabilities();
        */

        return new SPARQLSiteCapabilities();
    }

    //public String getType() { return "SPARQL"; }

    @Override
    public String toString() { return getURL().toString(); }
}
