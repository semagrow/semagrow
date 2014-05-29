package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

/**
 * Created by angel on 5/29/14.
 */
public class TransformIteration extends
        ConvertingIteration<BindingSet,BindingSet,QueryEvaluationException> {

    public TransformIteration(Iteration<? extends BindingSet, ? extends QueryEvaluationException> iter) {
        super(iter);
    }

    @Override
    protected BindingSet convert(BindingSet bindings) throws QueryEvaluationException {
        return bindings;
    }
}
