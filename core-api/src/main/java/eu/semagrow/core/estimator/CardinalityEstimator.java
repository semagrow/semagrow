package eu.semagrow.core.estimator;

import eu.semagrow.core.source.Site;
import org.openrdf.query.algebra.TupleExpr;


/**
 * Query Decomposer
 * 
 * <p>Interface for any component that estimates the number
 * of results expected when executing a given expression.</p>
 * 
 * @author Angelos Charalambidis
 */

public interface CardinalityEstimator
{

	/**
	 * This method estimates the cardinality of the results of executing
	 * {@code expr} without making reference to a specific data source.
	 * This method call is not valid for all types of expressions,
	 * as some expressions can only be estimated in reference to a
	 * specific data source. 
	 * @param expr
	 * @return
	 */
    long getCardinality( TupleExpr expr );

    /**
	 * This method estimates the cardinality of the results of executing
	 * {@code expr} at data source {@code source}.
     * @param expr
     * @param source
     * @return
     */
    long getCardinality( TupleExpr expr, Site source );
}
