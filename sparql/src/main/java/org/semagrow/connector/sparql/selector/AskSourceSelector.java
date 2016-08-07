package org.semagrow.connector.sparql.selector;

import org.semagrow.art.Loggable;
import org.semagrow.selector.Site;
import org.semagrow.selector.SourceMetadata;
import org.semagrow.selector.SourceSelector;
import org.semagrow.selector.SourceSelectorWrapper;
import org.semagrow.connector.sparql.SPARQLSite;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.semagrow.connector.sparql.execution.SPARQLRepository;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;


/**
 * ASK Source Selector.
 * 
 * <p>Implementation of SourceSelector that tries to execute ASK queries to identify
 * the data sources that hold triples that match the given triple patterns. This class
 * extends SourceSelectorWrapper, and thus relies on a wrapped SourceSelector that
 * provides the initial list of candidate data sources. 
 *  
 * <p>Note that if any exceptions are thrown when connecting to the remote data sources,
 * this SourceSelector simples returns "true" (matching triples exist). This avoids
 * rejecting data sources that hold relevant triples because of transient errors.
 *  
 * @author Antonios Troumpoukis
 * @author Stasinos Konstantopoulos
 */


public class AskSourceSelector extends SourceSelectorWrapper implements SourceSelector
{

	private org.slf4j.Logger logger =
			org.slf4j.LoggerFactory.getLogger( AskSourceSelector.class );


	static private ExecutorService executor;

	public AskSourceSelector( SourceSelector selector ) {

		super( selector );
		executor = Executors.newCachedThreadPool();
	}


	/*
	 * SourceSelector IMPLEMENTATION
	 */


	@Override
	public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings )
	{
		Collection<SourceMetadata> list = super.getSources( pattern, dataset, bindings );
		return restrictSourceList( pattern, list );
	}

	private Collection<SourceMetadata> getSources( Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings )
	{
		Collection<SourceMetadata> list = new LinkedList<SourceMetadata>();
		for( StatementPattern p : patterns ) {
			list.addAll( this.getSources(p, dataset, bindings) );
		}
		return list;
	}

	@Override
	public Collection<SourceMetadata> getSources( TupleExpr expr, Dataset dataset, BindingSet bindings )
	{
		if( expr instanceof StatementPattern ) {
			return getSources((StatementPattern)expr, dataset, bindings);
		}

		Collection<StatementPattern> patterns  = StatementPatternCollector.process( expr );
		return getSources( patterns, dataset, bindings );
	}



	/*
	 * PRIVATE HELPERS
	 */


	/**
	 * This method returns a list of {link SourceMetadata} objects that refer to data sources that
	 * contain at least one triple that matches {@code pattern}. The input {@code list} of candidate
	 * data sources is not modified.
	 * <p>
	 * This method is the entry point to the specific functionality of this class, and all the methods
	 * above that implement the SourceSelector interface must use this method. This allows all performance
	 * related logging to be implemented here.
	 * @param pattern The triple pattern that guides data source selection
	 * @param list The list of candidate data sources
	 * @return The subset of the data sources in list that contain triples matching the pattern
	 */
	 @Loggable
	 private Collection<SourceMetadata> restrictSourceList( StatementPattern pattern, Collection<SourceMetadata> list )
	 {
		 Collection<SourceMetadata> restrictedList = new LinkedList<SourceMetadata>();

		 Collection<Callable<SourceMetadata>> todo = new LinkedList<Callable<SourceMetadata>>();

		 for(SourceMetadata metadata : list) {
			 Collection<Site> sources = metadata.getSites();
			 SourceMetadata m = metadata;

			 Callable<SourceMetadata> f = () -> {
				 if (sources.iterator().next() instanceof SPARQLSite) {
					 boolean ask = askPattern(pattern, ((SPARQLSite) sources.iterator().next()).getURL(), false);
					 return ask ? m : null;
				 }
				 else {
					 return m;
				 }
			 };

			 todo.add(f);
		 }

		 try {
			 List<Future<SourceMetadata>> list1 = executor.invokeAll(todo);
			 for (Future<SourceMetadata> fut : list1) {
				 if (fut.isDone()) {
					 SourceMetadata m = null;
					 try {
						 m = fut.get();
					 } catch (ExecutionException e) {
						 logger.info( "AskSourceSelector Future execution", e);
					 }
					 if (m != null) {
						 restrictedList.add(m);
					 }
				 }
			 }

		 } catch (InterruptedException e) {
			 logger.info( "AskSourceSelector interrupted", e);
		 }

		 return restrictedList;
	 }


	 /**
	  * This method checks if a SPARQL endpoint serves at least one triple that matches {@pattern}.
	  * Not all endpoint implementations support ASK queries. If the ASK query fails, this method can
	  * fall back to querying {@code SELECT * WHERE { pattern } LIMIT 1. Note that his can be very slow
	  * for some endpoint implementations, unfortunately usually those that do not support ASK in the
	  * first place.
	  * @param pattern The triple pattern to check
	  * @param source The URL of the SPARQL endpoint
	  * @param boolean allow_select If true, then the method is allowed to fall back to SELECT
	  * @return false if it has been established that {@code source} does not contain any matching triples, true otherwise
	  */

	 private boolean askPattern(StatementPattern pattern, URL source, boolean allow_select )
	 {

		 boolean retv;

		 Value s = pattern.getSubjectVar().getValue();
		 Value p = pattern.getPredicateVar().getValue();
		 Value o = pattern.getObjectVar().getValue();

		 Repository rep;

		 retv = true;
		 rep = new SPARQLRepository( source.toString() );
		 RepositoryConnection conn = null;
		 try {
			 rep.initialize();
			 conn = rep.getConnection();
		 }
		 catch( org.eclipse.rdf4j.repository.RepositoryException ex ) {
			 // Failed to contact source
			 // Log a warnig and reply "true" just in case this is a transient failure
			 logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
					 pattern.toString(), ex.getMessage() );
		 }

		 if( conn!= null ) {
			 try {
				 retv = conn.hasStatement( (Resource)s, (IRI)p, o, true );
				 allow_select = false; // No need to use this any more
			 }
			 catch( org.eclipse.rdf4j.repository.RepositoryException ex ) {
				 // Failed to contact source
				 // Log a warnig and reply "true" just in case this is a transient failure
				 logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
						 pattern.toString(), ex.getMessage() );
			 }
		 }

		 if( allow_select && (conn!=null) ) {
			 String qs = "SELECT * WHERE { ?S ?P ?O } LIMIT 1";

			 TupleQuery q;
			 try {
				 q = conn.prepareTupleQuery( org.eclipse.rdf4j.query.QueryLanguage.SPARQL, qs );
				 if( s != null ) { q.setBinding( "S", s ); }
				 if( p != null ) { q.setBinding( "P", p ); }
				 if( o != null ) { q.setBinding( "O", o ); }
			 }
			 catch( org.eclipse.rdf4j.query.MalformedQueryException ex ) {
				 throw new AssertionError();
				 // ASSERTION ERROR: This can never happen
			 }
			 catch( org.eclipse.rdf4j.repository.RepositoryException ex ) {
				 // Failed to contact source
				 // Log a warning and reply "true" just in case this is a transient failure
				 logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
						 pattern.toString(), ex.getMessage() );
				 q = null;
			 }

			 try {
				 retv = q.evaluate().hasNext();
			 }
			 catch( org.eclipse.rdf4j.query.QueryEvaluationException ex ) {
				 // Failed to contact source
				 // Log a warnig and reply "true" just in case this is a transient failure
				 logger.warn( "Failed to contact source to execute query {}. Exception: {}",
						 q.toString(), ex.getMessage() );
			 }
			 catch( NullPointerException ex ) { /* NOOP: q was not prepared above */ }

		 }

		 try { conn.close(); }
		 catch( NullPointerException ex ) { /* NPOP: the connection failed to open above */ }
		 catch( org.eclipse.rdf4j.repository.RepositoryException ex ) { }

		 return retv;
	 }

}

