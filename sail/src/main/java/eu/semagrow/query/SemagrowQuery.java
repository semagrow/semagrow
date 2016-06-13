package eu.semagrow.query;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

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

    void addExcludedSource(IRI source);

    void addIncludedSource(IRI source);

    Collection<IRI> getExcludedSources();

    Collection<IRI> getIncludedSources();
    
    /**
     * This method changes the current thread's {link org.slf4j.MDC}
     * context into the context stored in this object.
     */
    public void setMDC();
}
