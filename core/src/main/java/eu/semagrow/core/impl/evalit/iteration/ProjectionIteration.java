package eu.semagrow.core.impl.evalit.iteration;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.ProjectionIterator;

/**
 * Projection iteration that is provenance aware
 */
public class ProjectionIteration  extends ProjectionIterator {

    public ProjectionIteration(Projection projection,
                               CloseableIteration<BindingSet, QueryEvaluationException> iter, BindingSet parentBindings)
            throws QueryEvaluationException {

        super(projection,iter,parentBindings);
    }

    @Override
    protected BindingSet convert(BindingSet bindings) throws QueryEvaluationException {
        if (bindings.hasBinding(EvaluationStrategyImpl.provenanceField)) {
            Binding b = bindings.getBinding(EvaluationStrategyImpl.provenanceField);
            BindingSet converted = super.convert(bindings);
            if (!converted.hasBinding(EvaluationStrategyImpl.provenanceField)) {
                QueryBindingSet convertedWithProv = new QueryBindingSet(converted);
                convertedWithProv.setBinding(b);
                converted = convertedWithProv;
            }
            return converted;
        }

        return super.convert(bindings);
    }
}
