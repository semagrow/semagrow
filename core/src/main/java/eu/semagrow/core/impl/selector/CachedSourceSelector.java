package eu.semagrow.core.impl.selector;

import eu.semagrow.art.LogExprProcessing;
import eu.semagrow.core.impl.planner.DPPlanOptimizer;
import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by angel on 10/6/2015.
 */
public class CachedSourceSelector extends SourceSelectorWrapper
{
    final private org.slf4j.Logger logger =
    		org.slf4j.LoggerFactory.getLogger( CachedSourceSelector.class );

    private Map<StatementPattern, List<SourceMetadata>> cache = new HashMap<>();

    public CachedSourceSelector(SourceSelector selector) {
        super(selector);
    }

    @Override
    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings)
    {
       	LogExprProcessing event = new LogExprProcessing();
       	logger.info( "START" );
        
        List<SourceMetadata> retv;

        if( cache.containsKey(pattern) ) {
            retv = cache.get(pattern);
        }
        else {
            List<SourceMetadata> list = super.getSources(pattern, dataset, bindings);
            cache.put(pattern, list);
            retv = list;
        }

        logger.info( "END" );
        event.finalize();
        return retv;
    }

    @Override
    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings)
    {
        List<SourceMetadata> list = new LinkedList<SourceMetadata>();
        for (StatementPattern p : patterns) {
            list.addAll(this.getSources(p, dataset, bindings));
        }
        return list;
    }


}
