package eu.semagrow.core.impl.selector;

import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import java.util.List;

/**
 * A simple wrapper of a @{link SourceSelector} that can be
 * used to remove all the boilerplate code in the inherited classes
 * @author Angelos Charalambidis
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
