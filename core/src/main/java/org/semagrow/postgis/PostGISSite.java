package org.semagrow.postgis;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;

public class PostGISSite implements Site {
	
	static final String TYPE = "POSTGIS";
		
    private final IRI endpoint;

    public PostGISSite(IRI endpoint) {
        this.endpoint = endpoint;
    }
    
    public Resource getID() { 
    	return getURI(); 
    }
    
    public String getType() { 
    	return TYPE; 
    }
    
    public IRI getURI() { 
    	return endpoint; 
    }

    @Override
    public boolean isRemote() {
        return endpoint != null;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PostGISSite) {
        	PostGISSite s = (PostGISSite)o;
            return (s.getURI().equals(this.getURI()));
        }
        return false;
    }
    
    @Override
    public SiteCapabilities getCapabilities() {
//    	return PostGISSchemaInit.getInstance().getPostGISSchema(endpoint);
        return new PostGISSiteCapabilities();
    }
    
//    public PostGISSchema getPostGISSchema() {
//        return PostGISSchemaInit.getInstance().getPostGISSchema(endpoint);
//    }
//
//    public String getBase() {
//        return getPostGISSchema().getBase();
//    }
//
//    public String getAddress() {
//        return getPostGISSchema().getAddress();
//    }
//
//    public int getPort() {
//        return getPostGISSchema().getPort();
//    }
//
//    public String getKeyspace() {
//        return getPostGISSchema().getKeyspace();
//    }
    
    @Override
    public String toString() { 
    	return endpoint.toString(); 
    }
}
