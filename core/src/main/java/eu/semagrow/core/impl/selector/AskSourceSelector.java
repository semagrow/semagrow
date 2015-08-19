package eu.semagrow.core.impl.selector;

import java.util.LinkedList;
import java.util.List;

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
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 * Created by antru on 1/27/15.
 */

public class AskSourceSelector extends SourceSelectorWrapper implements SourceSelector {

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
            boolean ask = true;     	
        	try {
        		ask = askPattern(pattern, sources.get(0));
        	} catch (RepositoryException e) {
        		ask = true;
        		e.printStackTrace();
        	}
        	if (ask)
            	restrictedList.add(m);
        }
        return restrictedList;
    }
	
    private boolean askPattern(StatementPattern pattern, URI source) throws RepositoryException {
    	
    	boolean ask;
    	
    	Value s = pattern.getSubjectVar().getValue();
	String ss = (s==null)? null : s.stringValue();

    	Value p = pattern.getPredicateVar().getValue();
	String sp = (p==null)? null : p.stringValue();

    	Value o = pattern.getObjectVar().getValue();
	String so = (o==null)? null : o.stringValue();

	String qs = "SELECT ?S ?P ?O WHERE { ?S ?P ?P } LIMIT 1";
    	Repository rep = new SPARQLRepository(source.stringValue());
    	rep.initialize();
    	RepositoryConnection conn = rep.getConnection();
	try {
		TupleQuery q = conn.prepareTupleQuery( org.openrdf.query.QueryLanguage.SPARQL, qs );
		if( s != null ) { q.setBinding( "S", s ); }
		if( p != null ) { q.setBinding( "P", p ); }
		if( o != null ) { q.setBinding( "O", o ); }
		ask = q.evaluate().hasNext();
	}
	catch( org.openrdf.query.MalformedQueryException ex ) {
		throw new AssertionError();
		// ASSERTION ERROR: This can never happen
	}
	catch( org.openrdf.query.QueryEvaluationException ex ) {
		throw new RepositoryException( ex );
	}

    	conn.close();
    	
    	return ask;
    }
}

