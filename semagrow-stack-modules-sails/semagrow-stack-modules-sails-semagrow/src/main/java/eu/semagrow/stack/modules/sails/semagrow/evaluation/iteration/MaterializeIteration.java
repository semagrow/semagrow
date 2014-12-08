package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.file.MaterializationHandle;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.file.MaterializationManager;
import info.aduna.iteration.DelayedIteration;
import info.aduna.iteration.Iteration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResultHandlerException;

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
