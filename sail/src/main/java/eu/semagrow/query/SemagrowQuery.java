package eu.semagrow.query;

import org.openrdf.model.URI;
import org.openrdf.query.Query;
import org.openrdf.query.algebra.TupleExpr;

import java.util.Collection;


/**
 * Semagrow Query
 * 
 * <p>
 * This is the interface for all types of queries that Semagrow handles.
 * It specifies functionality relevant to query processing and to maintaining
 * the processing context within which processing events are logged. This is
 * necessary in order to allow Semagrow ART (eu.semagrow.monitor.art) to track
 * the events relevant to a given query processing across threads. 
 * </p> 
 *  
 * @author Angelos Charalambidis
 * @author Stasinos Konstantopoulos
 */

public interface SemagrowQuery extends Query
{

    TupleExpr getDecomposedQuery() ;

    void addExcludedSource(URI source);

    void addIncludedSource(URI source);

    Collection<URI> getExcludedSources();

    Collection<URI> getIncludedSources();
    
    /**
     * This method changes the current thread's {link org.slf4j.MDC}
     * context into the context stored in this object.
     */
    public void setMDC();
}
