package eu.semagrow.core.impl.evalit.iteration;

import eu.semagrow.core.impl.evaluation.file.MaterializationHandle;
import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import org.eclipse.rdf4j.common.iteration.DelayedIteration;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

import java.util.ArrayList;


/**
 * Created by angel on 10/21/14.
 */
public class MaterializeIteration extends DelayedIteration<BindingSet, QueryEvaluationException> {

    private Iteration<BindingSet, QueryEvaluationException> innerIter;
    private MaterializationManager manager;
    public MaterializeIteration(MaterializationManager manager,
                                Iteration<BindingSet,QueryEvaluationException> iter) {
        this.innerIter = iter;
        this.manager = manager;
    }

    @Override
    protected Iteration<? extends BindingSet, ? extends QueryEvaluationException>
        createIteration() throws QueryEvaluationException
    {

        MaterializationHandle handle = manager.saveResult();
        materializeIter(handle, innerIter);
        return manager.getResult(handle.getId());
    }

    private void materializeIter(MaterializationHandle handle, Iteration<BindingSet, QueryEvaluationException> iter)
        throws QueryEvaluationException
    {

        try {

            if (iter.hasNext()) {
                BindingSet b = iter.next();
                handle.startQueryResult(new ArrayList<String>(b.getBindingNames()));
                handle.handleSolution(b);
            }

            while (iter.hasNext()) {
                BindingSet b = iter.next();
                handle.handleSolution(b);
            }

            handle.endQueryResult();

        } catch (TupleQueryResultHandlerException e) {
            throw new QueryEvaluationException(e);
        }
    }
}
