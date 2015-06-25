package eu.semagrow.core.impl.planner;

import org.openrdf.query.algebra.ValueExpr;

import java.util.Collection;

/**
 * Created by angel on 10/8/14.
 */
public class DecomposerContext {

    Ordering ordering;

    public Collection<ValueExpr> filters;

    int limit;

}
