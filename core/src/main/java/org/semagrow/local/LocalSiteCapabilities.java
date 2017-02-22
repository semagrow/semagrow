package org.semagrow.local;

import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.semagrow.plan.Plan;
import org.semagrow.selector.AbstractSiteCapabilities;

/**
 * Created by angel on 14/6/2016.
 */
public class LocalSiteCapabilities extends AbstractSiteCapabilities {

    @Override
    public boolean acceptsFilter(Plan p1, ValueExpr cond) {
        if (cond instanceof FunctionCall) {
            return acceptsFunction((FunctionCall)cond);
        }
        else
            return true;
    }

    private boolean acceptsFunction(FunctionCall call) {
        return FunctionRegistry.getInstance().has(call.getURI());
    }

}
