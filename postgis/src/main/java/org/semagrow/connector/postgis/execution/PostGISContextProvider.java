package org.semagrow.connector.postgis.execution;

import java.util.stream.Stream;

//import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostGISContextProvider {
	
	private final static Logger logger = LoggerFactory.getLogger(PostGISContextProvider.class);
	
	String url;
    String username;
    String password;
	
	public PostGISContextProvider(String url, String username, String password) {
		this.url = url;
        this.username = username;
        this.password = password;
	}

    /**
     * Creates a database connection for data access.
     * @return DSLConext.
     */
//    private DSLContext dsl() {
//        return DSL.using(new PostGISConnectionProvider(this.url, this.username, this.password), 
//        		SQLDialect.MYSQL);
//    }
    
    public Stream<Record> execute(String query) {
    	logger.debug("execute!!!");
        logger.debug("Sending query: {}", query);
        Stream<Record> results = DSL.using(new PostGISConnectionProvider
        		(this.url, this.username, this.password), 
        		SQLDialect.POSTGRES).fetch(query).stream();
        return results;
    }
}
