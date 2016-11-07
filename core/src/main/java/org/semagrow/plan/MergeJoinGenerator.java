package org.semagrow.plan;

import org.semagrow.plan.operators.MergeJoin;
import org.semagrow.local.LocalSite;
import org.eclipse.rdf4j.query.algebra.Join;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A {@JoinImplGenerator} that creates Join trees using {@link MergeJoin}.
 * Both operant plans are enforced to the ordering that is needed from the {@link MergeJoin}.
 * @author acharal
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
