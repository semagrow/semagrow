package org.semagrow.plan;

import org.semagrow.plan.operators.HashJoin;
import org.semagrow.local.LocalSite;
import org.eclipse.rdf4j.query.algebra.Join;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A {@JoinImplGenerator} that creates Join trees using {@link HashJoin}
 * @author acharal
 */
class HashJoinGenerator implements JoinImplGenerator {

    @Override
    public Collection<Join> generate(Plan p1, Plan p2, PlanGenerationContext ctx) {

        Collection<Join> l = new LinkedList<Join>();

        Join expr = new HashJoin(ctx.enforce(p1, LocalSite.getInstance()), ctx.enforce(p2, LocalSite.getInstance()));

        l.add(expr);

        return l;
    }
}
