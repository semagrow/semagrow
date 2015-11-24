package eu.semagrow.core.evaluation;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;


/**
 * Query Executor
 * 
 * <p>Interface for any engine capable of serializing a tuple expression as an
 * appropriate query for the given remote data source and fetching results.</p>
 * 
 * @author Angelos Charalambidis
 */

public interface QueryExecutor
{

    void initialize();

    void shutdown();

    CloseableIteration<BindingSet,QueryEvaluationException>
    evaluate( URI endpoint, TupleExpr expr, BindingSet bindings )
    		throws QueryEvaluationException;

    CloseableIteration<BindingSet,QueryEvaluationException>
    evaluate( URI endpoint, TupleExpr expr, CloseableIteration<BindingSet,QueryEvaluationException> bindingIter )
    		throws QueryEvaluationException;

}
