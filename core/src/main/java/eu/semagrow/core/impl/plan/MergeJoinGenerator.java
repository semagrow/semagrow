package eu.semagrow.core.impl.plan;

import eu.semagrow.core.impl.plan.ops.MergeJoin;
import eu.semagrow.core.plan.*;
import eu.semagrow.core.source.LocalSite;
import org.openrdf.query.algebra.Join;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 31/3/2016.
 */
class MergeJoinGenerator implements JoinImplGenerator {

    @Override
    public Collection<Join> generate(Plan p1, Plan p2, PlanGenerationContext ctx) {

        Collection<Join> l = new LinkedList<Join>();

        Ordering o = null;

        Join expr = new MergeJoin(ctx.enforce(ctx.enforce(p1,o), LocalSite.getInstance()), ctx.enforce(ctx.enforce(p2,o), LocalSite.getInstance()));

        l.add(expr);

        return l;
    }
}
