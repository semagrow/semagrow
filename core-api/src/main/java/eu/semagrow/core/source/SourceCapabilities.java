package eu.semagrow.core.source;

import eu.semagrow.core.plan.Plan;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;

import java.util.Set;

/**
 * Created by angel on 31/3/2016.
 */
public interface SourceCapabilities {

    boolean canExecute(Plan p);

    boolean isJoinable(Plan p1, Plan p2);

    boolean acceptsBindings(Plan p1, Set<String> var);

    boolean acceptsFilter(Plan p1, ValueExpr cond);

    TupleExpr enforceSite(Plan p1an);

}
