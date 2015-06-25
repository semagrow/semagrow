package eu.semagrow.core.impl.evaluation.iteration;

import eu.semagrow.core.impl.evaluation.EvaluationStrategyImpl;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.iterator.ProjectionIterator;

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
