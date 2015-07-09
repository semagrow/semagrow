package eu.semagrow.core.impl.evaluation.util;

import eu.semagrow.core.impl.evaluation.rx.EvaluationStrategy;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import java.util.Collection;


/**
 * Created by angel on 9/7/2015.
 */
public class EvalUtils {


    public static BindingSet extend(EvaluationStrategy strategy, Collection<ExtensionElem> extElems, BindingSet sourceBindings)
            throws QueryEvaluationException
    {
        QueryBindingSet targetBindings = new QueryBindingSet(sourceBindings);

        for (ExtensionElem extElem : extElems) {
            ValueExpr expr = extElem.getExpr();
            if (!(expr instanceof AggregateOperator)) {
                try {
                    // we evaluate each extension element over the targetbindings, so that bindings from
                    // a previous extension element in this same extension can be used by other extension elements.
                    // e.g. if a projection contains (?a + ?b as ?c) (?c * 2 as ?d)
                    Value targetValue = strategy.evaluate(extElem.getExpr(), targetBindings);

                    if (targetValue != null) {
                        // Potentially overwrites bindings from super
                        targetBindings.setBinding(extElem.getName(), targetValue);
                    }
                } catch (ValueExprEvaluationException e) {
                    // silently ignore type errors in extension arguments. They should not cause the
                    // query to fail but just result in no additional binding.
                }
            }
        }

        return targetBindings;
    }

    public static BindingSet project(ProjectionElemList projElemList,
                                     BindingSet sourceBindings,
                                     BindingSet parentBindings)
    {
        QueryBindingSet resultBindings = new QueryBindingSet(parentBindings);

        for (ProjectionElem pe : projElemList.getElements()) {
            Value targetValue = sourceBindings.getValue(pe.getSourceName());
            if (targetValue != null) {
                // Potentially overwrites bindings from super
                resultBindings.setBinding(pe.getTargetName(), targetValue);
            }
        }

        return resultBindings;
    }
}
