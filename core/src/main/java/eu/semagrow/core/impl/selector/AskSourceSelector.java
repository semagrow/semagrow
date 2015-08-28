package eu.semagrow.core.impl.selector;

import eu.semagrow.art.LogExprProcessing;
import eu.semagrow.commons.algebra.QueryRoot;
import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLRepository;

import java.util.LinkedList;
import java.util.List;


/**
 * ASK Source Selector.
 * 
 * <P>Implementation of SourceSelector that tries to execute ASK queries to identify
 * the data sources that hold triples that match the given triple patterns. This class
 * extends SourceSelectorWrapper, and thus relies on a wrapped SourceSelector that
 * provides the initial list of candidate data sources. 
 *  
 * <P>Note that if any exceptions are thrown when connecting to the remote data sources,
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

	public AskSourceSelector( SourceSelector selector ) {
		super( selector );
	}

	public AskSourceSelector( SourceSelector selector, int cacheSize ) {
		super( selector );
	}



	/*
	 * SourceSelector IMPLEMENTATION
	 */



	@Override
	public List<SourceMetadata> getSources( StatementPattern pattern, Dataset dataset, BindingSet bindings )
	{
		List<SourceMetadata> list = super.getSources( pattern, dataset, bindings );
		return restrictSourceList( pattern, list );
	}

	@Override
	public List<SourceMetadata> getSources( Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings )
	{
		List<SourceMetadata> list = new LinkedList<SourceMetadata>();
		for( StatementPattern p : patterns ) {
			list.addAll( this.getSources(p, dataset, bindings) );
		}
		return list;
	}

	@Override
	public List<SourceMetadata> getSources( TupleExpr expr, Dataset dataset, BindingSet bindings )
	{
		if( expr instanceof StatementPattern ) {
			return getSources((StatementPattern)expr, dataset, bindings);
		}

		List<StatementPattern> patterns  = StatementPatternCollector.process( expr );
		return getSources( patterns, dataset, bindings );
	}



	/*
	 * PRIVATE HELPERS
	 */


	/**
	 * This method returns a list of {link SourceMetadata} objects that refer to data sources that
	 * contain at least one triple that matches {@pattern}. The input {@code list} of candidate
	 * data sources is not modified.
	 * <p>
	 * This method is the entry point to the specific functionality of this class, and all the methods
	 * above that implement the SourceSelector interface must use this method. This allows all performance
	 * related logging to be implemented here.   
	 * @param pattern The triple pattern that guides data source selection
	 * @param list The list of candidate data sources
	 * @return The subset of the data sources in list that contain triples matching the pattern 
	 */

	 private List<SourceMetadata> restrictSourceList( StatementPattern pattern, List<SourceMetadata> list )
	 {
		 LogExprProcessing logEvent = LogExprProcessing.create( pattern );
		 logger.info( "START" );

		 List<SourceMetadata> restrictedList = new LinkedList<SourceMetadata>();

		 for( SourceMetadata metadata : list ) {
			 List<URI> sources = metadata.getEndpoints();
			 SourceMetadata m = metadata;
			 boolean ask = askPattern( pattern, sources.get(0), false );
			 if( ask ) { restrictedList.add( m ); }
		 }
		 
		 logger.info( "END" );
		 logEvent.finalize();
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

	 private boolean askPattern( StatementPattern pattern, URI source, boolean allow_select )
	 {

		 boolean retv;

		 Value s = pattern.getSubjectVar().getValue();
		 Value p = pattern.getPredicateVar().getValue();
		 Value o = pattern.getObjectVar().getValue();

		 Repository rep;

		 retv = true;
		 rep = new SPARQLRepository( source.stringValue() );
		 RepositoryConnection conn = null;
		 try { 
			 rep.initialize();
			 conn = rep.getConnection();
		 }
		 catch( org.openrdf.repository.RepositoryException ex ) {
			 // Failed to contact source
			 // Log a warnig and reply "true" just in case this is a transient failure
			 logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
					 pattern.toString(), ex.getMessage() );
		 }

		 if( conn!= null ) {
			 try { 
				 retv = conn.hasStatement( (Resource)s, (URI)p, o, true );
				 allow_select = false; // No need to use this any more
			 }
			 catch( org.openrdf.repository.RepositoryException ex ) {
				 // Failed to contact source
				 // Log a warnig and reply "true" just in case this is a transient failure
				 logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
						 pattern.toString(), ex.getMessage() );
			 }
		 }

		 if( allow_select && (conn!=null) ) {
			 String qs = "SELECT * WHERE { ?S ?P ?P } LIMIT 1";

			 TupleQuery q;
			 try {
				 q = conn.prepareTupleQuery( org.openrdf.query.QueryLanguage.SPARQL, qs );
				 if( s != null ) { q.setBinding( "S", s ); }
				 if( p != null ) { q.setBinding( "P", p ); }
				 if( o != null ) { q.setBinding( "O", o ); }
			 }
			 catch( org.openrdf.query.MalformedQueryException ex ) {
				 throw new AssertionError();
				 // ASSERTION ERROR: This can never happen
			 }
			 catch( org.openrdf.repository.RepositoryException ex ) {
				 // Failed to contact source
				 // Log a warning and reply "true" just in case this is a transient failure
				 logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
						 pattern.toString(), ex.getMessage() );
				 q = null;
			 }

			 try {
				 retv = q.evaluate().hasNext();
			 }
			 catch( org.openrdf.query.QueryEvaluationException ex ) {
				 // Failed to contact source
				 // Log a warnig and reply "true" just in case this is a transient failure
				 logger.warn( "Failed to contact source to execute query {}. Exception: {}",
						 q.toString(), ex.getMessage() );
			 }
			 catch( NullPointerException ex ) { /* NOOP: q was not prepared above */ }

		 }

		 try { conn.close(); }
		 catch( NullPointerException ex ) { /* NPOP: the connection failed to open above */ }
		 catch( org.openrdf.repository.RepositoryException ex ) { }

		 return retv;
	 }

}

