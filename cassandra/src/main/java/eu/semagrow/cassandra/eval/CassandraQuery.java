package eu.semagrow.cassandra.eval;

import java.util.Map;

/**
 * Created by angel on 21/4/2016.
 */
public class CassandraQuery {

    String strQuery;

    Map<String, String> var2column;

    String subject;

    public CassandraQuery(String subject, String query, Map<String, String> var2column) {
        this.strQuery = query;
        this.subject = subject;
        this.var2column = var2column;
    }
}
