package org.semagrow.plan;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
// FIXME: import org.semagrow.algebra.QueryRoot instead

/**
 * Query planner is the main interface that specifies
 * the compilation of a logical expression given a set of bindings and
 * a list of referred datasets into a query evaluation plan.
 * @author acharal
 * @since 2.0
 */
public interface QueryCompiler {


    /**
    * Compiles a query into a executable plan
    * @param query the logical query
    * @param dataset the default and named graphs of the query
    * @param bindings potential bindings of some of the variables in {@code query}
    * @return an executable query plan
    */
    Plan compile(QueryRoot query, Dataset dataset, BindingSet bindings);

}
