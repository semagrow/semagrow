package eu.semagrow.core.impl.evalit.iteration;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.GroupIterator;

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
