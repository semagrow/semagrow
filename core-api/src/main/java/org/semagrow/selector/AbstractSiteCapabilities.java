package org.semagrow.selector;

import org.semagrow.plan.Plan;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;

import java.util.Set;

/**
 * Created by angel on 31/3/2016.
 */
public abstract class AbstractSiteCapabilities implements SiteCapabilities {

    @Override
    public boolean canExecute(Plan p) {
        return true;
    }

    @Override
    public boolean isJoinable(Plan p1, Plan p2) {
        return true;
    }

    @Override
    public boolean acceptsBindings(Plan p1, Set<String> var) {
        return true;
    }

    @Override
    public boolean acceptsFilter(Plan p1, ValueExpr cond) {
        return true;
    }

    @Override
    public TupleExpr enforceSite(Plan p1) {
        return p1;
    }

}
