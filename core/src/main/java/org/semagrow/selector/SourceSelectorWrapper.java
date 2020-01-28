package org.semagrow.selector;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.Collection;

/**
 * A simple wrapper of a @{link SourceSelector} that can be
 * used to remove all the boilerplate code in the inherited classes
 * @author Angelos Charalambidis
 */
public class SourceSelectorWrapper  implements SourceSelector, QueryAwareSourceSelector {

    private SourceSelector wrappedSelector;

    public SourceSelectorWrapper(SourceSelector selector) {
        assert selector != null;
        wrappedSelector = selector;
    }

    public SourceSelector getWrappedSelector() { return wrappedSelector; }

    @Override
    public void setSiteResolver(SiteResolver siteResolver) {
        getWrappedSelector().setSiteResolver(siteResolver);
    }

    public Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return getWrappedSelector().getSources(pattern, dataset, bindings);
    }

    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings) {
        return getWrappedSelector().getSources(expr, dataset, bindings);
    }

    @Override
    public void processTupleExpr(TupleExpr expr) {
        if (wrappedSelector instanceof QueryAwareSourceSelector) {
            ((QueryAwareSourceSelector) wrappedSelector).processTupleExpr(expr);
        }
    }
}
