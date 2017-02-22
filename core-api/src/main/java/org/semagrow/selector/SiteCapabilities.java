package org.semagrow.selector;

import org.semagrow.plan.Plan;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

import java.util.Set;

/**
 * Created by angel on 31/3/2016.
 */
public interface SiteCapabilities {

    boolean canExecute(Plan p);

    boolean isJoinable(Plan p1, Plan p2);

    boolean acceptsBindings(Plan p1, Set<String> var);

    boolean acceptsFilter(Plan p1, ValueExpr cond);

    TupleExpr enforceSite(Plan p1);

}
