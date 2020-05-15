package org.semagrow.connector.postgis;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;
import org.eclipse.rdf4j.model.Resource;

import java.net.URL;

public class PostGISSite implements Site {
	
	static final String TYPE = "POSTGIS";
		
    private URL endpointURI;

    public PostGISSite(URL uri) {
        assert uri != null;
        this.endpointURI = uri;
    }

    @Override
    public boolean isRemote() {
        return endpointURI != null;
    }

    public URL getURL() { 
    	return endpointURI; 
    }

    public Resource getID() { 
    	return SimpleValueFactory.getInstance().createURI(getURL().toString()); 
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PostGISSite) {
        	PostGISSite s = (PostGISSite)o;
            return (s.getURL().equals(this.getURL()));
        }
        return false;
    }

    public String getType() { 
    	return TYPE; 
    }

    @Override
    public SiteCapabilities getCapabilities() {

        /*
        // FIXME
        if (endpointURI != null && endpointURI.stringValue().equals("http://my.cassandra.antru/"))
            return new CassandraCapabilities();
        */

        return new PostGISSiteCapabilities();
    }

    //public String getType() { return "SPARQL"; }

    @Override
    public String toString() { 
    	return getURL().toString(); 
    }
}
