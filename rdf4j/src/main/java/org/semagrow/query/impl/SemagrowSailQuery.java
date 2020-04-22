package org.semagrow.query.impl;

import org.semagrow.art.LogUtils;
import org.semagrow.query.SemagrowQuery;
import org.semagrow.sail.SemagrowSailConnection;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.repository.sail.SailQuery;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.semagrow.algebra.QueryRoot;

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

    private final Set<IRI> excludedSources;
    private final Set<IRI> includeOnlySources;
    
    /* a copy of the MDC context map at the time of object instantiation */ 
    private final java.util.Map<String,String> contextMap;

    protected SemagrowSailQuery( ParsedQuery parsedQuery, SailRepositoryConnection con )
    {
        super( parsedQuery, con );

        this.excludedSources = new HashSet<IRI>();
        this.includeOnlySources = new HashSet<IRI>();


    	/* All TupleExpr instances that are QueryRoot root instances must point back
    	 * to a SemagrowSailTupleQuery. To achieve this, this constructor should be
    	 * the only way through which QueryRoot is instantiated.
    	 */
        TupleExpr tupleExpr = this.getParsedQuery().getTupleExpr();
        tupleExpr = new QueryRoot( tupleExpr, this );
        this.getParsedQuery().setTupleExpr( tupleExpr );
        
        /* Each query processing run is logged within a processing context which, most
         * importantly, includes a UUID. This context is preserved even when processing
         * spans multiple threads retrieved form a thread pool.
         * The context across each thread is kept using MDC. Instantiating a
         * SemagrowSailTupleQuery sets a new context for this thread; this instance
         * maintains a copy, which is passed to worker threads.
         */
        LogUtils.setMDCifNull();

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

    public void addExcludedSource(IRI source) { excludedSources.add(source); }

    public void addIncludedSource(IRI source) { includeOnlySources.add(source); }

    public Collection<IRI> getExcludedSources() { return excludedSources; }

    public Collection<IRI> getIncludedSources() { return includeOnlySources; }

	@Override
	public void setMDC()
	{ org.slf4j.MDC.setContextMap( this.contextMap ); }
}
