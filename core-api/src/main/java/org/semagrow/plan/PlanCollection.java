package org.semagrow.plan;


import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.Collection;
import java.util.Set;

/**
 * @author acharal
 */
public interface PlanCollection extends Collection<Plan> {

    Set<TupleExpr> getLogicalExpr();

}
