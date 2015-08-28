package eu.semagrow.query.impl;

import eu.semagrow.query.SemagrowQuery;
import eu.semagrow.sail.SemagrowSailConnection;

import org.openrdf.model.URI;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.repository.sail.SailQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


/**
 * Semagrow Sail Query
 * 
 * <p>
 * This is the base class of all types of queries that the Semagrow Sail handles.
 * </p> 
 *  
 * @author Angelos Charalambidis
 * @author Stasinos Konstantopoulos
 */

public class SemagrowSailQuery extends SailQuery implements SemagrowQuery
{

    private final Set<URI> excludedSources;
    private final Set<URI> includeOnlySources;
    
    /* a copy of the MDC context map at the time of object instantiation */ 
    private final java.util.Map<String,String> contextMap;

    protected SemagrowSailQuery( ParsedQuery parsedQuery, SailRepositoryConnection con )
    {
        super( parsedQuery, con );

        this.excludedSources = new HashSet<URI>();
        this.includeOnlySources = new HashSet<URI>();


    	/* All TupleExpr instances that are QueryRoot root instances must point back
    	 * to a SemagrowSailTupleQuery. To achieve this, this constructor should be
    	 * the only way through which QueryRoot is instantiated.
    	 */
        TupleExpr tupleExpr = this.getParsedQuery().getTupleExpr();
        tupleExpr = new eu.semagrow.commons.algebra.QueryRoot( tupleExpr, this );
        this.getParsedQuery().setTupleExpr( tupleExpr );
        
        /* Each query processing run is logged within a processing context which, most
         * importantly, includes a UUID. This context is preserved even when processing
         * spans multiple threads retrieved form a thread pool.
         * The context across each thread is kept using MDC. Instantiating a
         * SemagrowSailTupleQuery sets a new context for this thread; this instance
         * maintains a copy, which is passed to worker threads.
         */
        org.slf4j.MDC.put( "uuid", UUID.randomUUID().toString() );

        this.contextMap = org.slf4j.MDC.getCopyOfContextMap();
    }

    public TupleExpr getDecomposedQuery() {

        SemagrowSailConnection conn = (SemagrowSailConnection) getConnection().getSailConnection();
        TupleExpr initialExpr = getParsedQuery().getTupleExpr();
        TupleExpr expr = initialExpr.clone();
        Dataset dataset = getDataset();

        if (dataset == null) {
            // No external dataset specified, use query's own dataset (if any)
            dataset = getParsedQuery().getDataset();
        }
        return conn.decompose(expr, dataset, getBindings(), getIncludedSources(), getExcludedSources());
    }

    public void addExcludedSource(URI source) { excludedSources.add(source); }

    public void addIncludedSource(URI source) { includeOnlySources.add(source); }

    public Collection<URI> getExcludedSources() { return excludedSources; }

    public Collection<URI> getIncludedSources() { return includeOnlySources; }

	@Override
	public void setMDC()
	{ org.slf4j.MDC.setContextMap( this.contextMap ); }
}
