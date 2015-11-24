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
	 * This method 
	 * @param expr
	 * @param bindings
	 * @param dataset
	 * @return
	 */

	Plan getBestPlan( TupleExpr expr, BindingSet bindings, Dataset dataset );

}
