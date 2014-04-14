package eu.semagrow.stack.modules.utils;

/**
 * Represents a SPARQL query to the Semagrow stack.
 * 
 * @author Georgios Giannakopoulos
 */
public class Query {
    protected String Query;

    public Query(String Query) {
        this.Query = Query;
    }

    @Override
    public String toString() {
        return Query;
    }
  
    
}
