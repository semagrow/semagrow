package eu.semagrow.stack.modules.sails.semagrow.selector;

import java.util.LinkedList;
import java.util.List;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;

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
    	Value p = pattern.getPredicateVar().getValue();
    	Value o = pattern.getObjectVar().getValue();
    	
    	Repository rep = new SPARQLRepository(source.stringValue());
    	rep.initialize();
    	RepositoryConnection conn = rep.getConnection();
    	
    	ask = conn.hasStatement((Resource)s,(URI)p,o,true);

    	conn.close();
    	
    	return ask;
    }
}

