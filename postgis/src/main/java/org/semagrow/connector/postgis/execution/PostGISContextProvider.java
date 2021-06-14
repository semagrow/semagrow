package org.semagrow.connector.postgis.execution;

import java.util.stream.Stream;

//import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class PostGISContextProvider {
		
	String url;
    String username;
    String password;
	
	public PostGISContextProvider(String url, String username, String password) {
		this.url = url;
        this.username = username;
        this.password = password;
	}
    
    public Stream<Record> execute(String query) {
        Stream<Record> results = DSL.using(new PostGISConnectionProvider
        		(this.url, this.username, this.password), 
        		SQLDialect.POSTGRES).fetch(query).stream();
        return results;
    }
}
