package org.semagrow.connector.postgis;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostGISSite implements Site {
	
	static final String TYPE = "POSTGIS";

    private IRI endpointURI;
    private String endpoint;
    private String username;
    private String password;
    private String dbname;

    public PostGISSite(IRI uri) {
        assert uri != null;
        this.endpointURI = uri;

        String regex = "^postgis://([^:]+):([^@]+)@([^ ]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(uri.toString());

        if (matcher.find()) {
            this.username = matcher.group(1);
            this.password = matcher.group(2);
            this.endpoint = "jdbc:postgresql://" + matcher.group(3);
//            this.dbname = matcher.group(4);	// is this correct ???
        }
        else {
            this.username = "postgres";
            this.password = "postgres";
            this.endpoint = uri.toString().replace("postgis://", "jdbc:postgresql://");
            this.dbname = uri.toString().substring(uri.toString().lastIndexOf("/") + 1);
        }
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
    
    public String getDatabaseName() {
        return dbname;
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
