package eu.semagrow.core.plan;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;


/**
 * Query Decomposer
 * 
 * <p>Interface for any component that receives a parsed query and
 * produces a query execution plan.</p>
 * 
 * @author Angelos Charalambidis
 */


public interface QueryDecomposer
{

	/**
	 * This method alters expr in place so that it becomes an execution plan.
	 * @param expr The expression that will be decomposed.
	 * @param dataset Specifies the dataset against which to execute an operation.
	 * @param bindings Bindings that must be set on this query.
	 */

	void decompose( TupleExpr expr, Dataset dataset, BindingSet bindings );

}
