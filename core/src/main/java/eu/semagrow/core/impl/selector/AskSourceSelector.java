package eu.semagrow.core.impl.selector;

/**
 * Created by antru on 1/27/15.
 */

import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLRepository;

import java.util.LinkedList;
import java.util.List;

public class AskSourceSelector extends SourceSelectorWrapper implements SourceSelector
{

  static private org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger( AskSourceSelector.class );


	public AskSourceSelector(SourceSelector selector) {
        super(selector);
	}
	
	public AskSourceSelector(SourceSelector selector, int cacheSize) {
		super(selector);
	}
	
	@Override
	public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
		List<SourceMetadata> list = super.getSources(pattern, dataset, bindings);
		return restrictSourceList(pattern, list);
	}

	@Override
	public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
		List<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for (StatementPattern p : patterns) {
            list.addAll(this.getSources(p, dataset, bindings));
        }
        return list;
	}

	@Override
	public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
		if (expr instanceof StatementPattern)
            return getSources((StatementPattern)expr, dataset, bindings);

        List<StatementPattern> patterns  = StatementPatternCollector.process(expr);
        return getSources(patterns, dataset, bindings);
	}
    
	private List<SourceMetadata> restrictSourceList(StatementPattern pattern, List<SourceMetadata> list) {
        List<SourceMetadata> restrictedList = new LinkedList<SourceMetadata>();

        for (SourceMetadata metadata : list) {
            List<URI> sources = metadata.getEndpoints();
            SourceMetadata m = metadata;
            boolean ask = askPattern( pattern, sources.get(0), true, false );
	    if( ask ) { restrictedList.add(m); }
        }
        return restrictedList;
    }
	
  private boolean askPattern( StatementPattern pattern, URI source, boolean allow_ask, boolean allow_limit )
  {

    boolean retv;

    Value s = pattern.getSubjectVar().getValue();
    Value p = pattern.getPredicateVar().getValue();
    Value o = pattern.getObjectVar().getValue();

    Repository rep;

    if( allow_ask || allow_limit ) {
      retv = true;
      rep = new SPARQLRepository(source.stringValue());
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

      if( allow_ask && (conn!=null) ) {
	try { 
	  retv = conn.hasStatement( (Resource)s, (URI)p, o, true );
	  allow_limit = false; // No need to use this any more
	}
	catch( org.openrdf.repository.RepositoryException ex ) {
	  // Failed to contact source
	  // Log a warnig and reply "true" just in case this is a transient failure
	  logger.warn( "Failed to contact source to ASK about pattern {}. Exception: {}",
		       pattern.toString(), ex.getMessage() );
	}
      }

      if( allow_limit && (conn!=null) ) {
	String ss = (s==null)? null : s.stringValue();
	String sp = (p==null)? null : p.stringValue();
	String so = (o==null)? null : o.stringValue();
	String qs = "SELECT ?S ?P ?O WHERE { ?S ?P ?P } LIMIT 1";

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
	  // Log a warnig and reply "true" just in case this is a transient failure
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

    } // endif allow_ask || alllow_limit
    else {
      // nothing I can do to double-check a source
      // just say "true"
      retv = true;
    }	

    return retv;
  }

}

