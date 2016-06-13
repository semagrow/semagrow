package eu.semagrow.core.impl.plan;

import eu.semagrow.core.impl.plan.ops.HashJoin;
import eu.semagrow.core.plan.JoinImplGenerator;
import eu.semagrow.core.plan.Plan;
import eu.semagrow.core.plan.PlanGenerationContext;
import eu.semagrow.core.source.LocalSite;
import org.eclipse.rdf4j.query.algebra.Join;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 31/3/2016.
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
