package org.semagrow.selector;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by angel on 20/6/2016.
 */
public abstract class PatternWiseSourceSelector implements SourceSelector {

    public abstract Collection<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindingSet);

    public Collection<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindingSet) {
        if (expr instanceof StatementPattern)
            return getSources((StatementPattern)expr, dataset, bindingSet);
        else {
            Collection<StatementPattern> patterns = StatementPatternCollector.process(expr);
            return patterns.stream().distinct()
                    .flatMap(p -> getSources(p, dataset, bindingSet).stream())
                    .collect(Collectors.toList());
        }
    }
}
