package org.semagrow.connector.postgis;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;

public class PostGISSite implements Site {
	
	static final String TYPE = "POSTGIS";

    private IRI endpointURI;
    private String endpoint;

    private final String username = "postgres";
    private final String password = "postgres";

    public PostGISSite(IRI uri) {
        assert uri != null;
        this.endpointURI = uri;
        this.endpoint = uri.toString().replace("postgis://","jdbc:postgresql://");
    }
    
    @Override
    public boolean isRemote() {
        return endpointURI != null;
    }

    public Resource getID() { 
    	return endpointURI;
    }
    
    public String getType() { 
    	return TYPE; 
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PostGISSite) {
        	PostGISSite s = (PostGISSite)o;
            return (s.getID().equals(this.getID()));
        }
        return false;
    }
    
    @Override
    public SiteCapabilities getCapabilities() {
        return new PostGISSiteCapabilities();
    }
    
    @Override
    public String toString() { 
    	return getID().toString();
    }
    
}
