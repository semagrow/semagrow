package eu.semagrow.core.impl.evaluation.iteration;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Group;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.iterator.GroupIterator;

/**
 * Created by angel on 9/28/14.
 */
public class GroupOrderedIteration extends GroupIterator {

    public GroupOrderedIteration(EvaluationStrategy strategy,
                                 Group group,
                                 BindingSet parentBindings) throws QueryEvaluationException {
        super(strategy, group, parentBindings);

    }

}
