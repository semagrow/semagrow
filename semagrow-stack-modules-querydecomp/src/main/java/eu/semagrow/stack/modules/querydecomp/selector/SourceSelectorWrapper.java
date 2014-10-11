package eu.semagrow.stack.modules.querydecomp.selector;

import eu.semagrow.stack.modules.api.source.SourceMetadata;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import java.util.List;

/**
 * Created by angel on 6/19/14.
 */
public class SourceSelectorWrapper  implements SourceSelector {

    private SourceSelector wrappedSelector;

    public SourceSelectorWrapper(SourceSelector selector) {
        assert selector != null;
        wrappedSelector = selector;
    }

    public SourceSelector getWrappedSelector() { return wrappedSelector; }

    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return getWrappedSelector().getSources(pattern, dataset, bindings);
    }

    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        return getWrappedSelector().getSources(patterns, dataset, bindings);
    }

    public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        return getWrappedSelector().getSources(expr, dataset, bindings);
    }
}
