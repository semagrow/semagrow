package eu.semagrow.core.impl.planner;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;

/**
 * Plan Optimizer
 * 
 * <p>Interface for any component that optimizes
 * a query execution plan.</p>
 * 
 * @author Angelos Charalambidis
 */

public interface PlanOptimizer
{

	/**
	 * Returns a Plan from an abstract expression consulting the bindings and dataset
	 * that accompanies the expression.
	 * @param expr the abstract expression that constitute the query
	 * @param bindings the BindingSet populated by the user
	 * @param dataset the datasets used in the query
	 * @return
	 */
	Plan getBestPlan( TupleExpr expr, BindingSet bindings, Dataset dataset );

}
