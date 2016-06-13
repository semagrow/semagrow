package eu.semagrow.core.evalit;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;


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
    evaluate( IRI endpoint, TupleExpr expr, BindingSet bindings )
    		throws QueryEvaluationException;

    CloseableIteration<BindingSet,QueryEvaluationException>
    evaluate( IRI endpoint, TupleExpr expr, CloseableIteration<BindingSet,QueryEvaluationException> bindingIter )
    		throws QueryEvaluationException;

}
