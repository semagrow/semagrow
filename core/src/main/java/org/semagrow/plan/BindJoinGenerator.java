package org.semagrow.plan;

import org.semagrow.plan.operators.BindJoin;
import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.local.LocalSite;
import org.semagrow.selector.Site;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Union;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

/**
 * A {@link JoinImplGenerator} that creates Join trees using {@link BindJoin}
 * @author acharal
 */
class BindJoinGenerator implements JoinImplGenerator {

    @Override
    public Collection<Join> generate(Plan p1, Plan p2, PlanGenerationContext ctx) {

        Collection<Join> l = new LinkedList<Join>();

        if (isBindable(p2, p1.getBindingNames())) {
            Join expr = new BindJoin(ctx.enforce(p1, LocalSite.getInstance()), ctx.enforce(p2, LocalSite.getInstance()));
            l.add(expr);
        }

        return l;
    }

    private boolean isBindable(Plan p, Set<String> bindingNames) {

        IsBindableVisitor v = new IsBindableVisitor();

        p.visit(v);
        if( v.condition ) {
            Site s = p.getProperties().getSite();
            if (s.isRemote()) {
                return s.getCapabilities().acceptsBindings(p, bindingNames);
            }
        }

        return v.condition;
    }


    private class IsBindableVisitor extends AbstractPlanVisitor<RuntimeException> {
        boolean condition = false;

        @Override
        public void meet(Union union) {
            union.getLeftArg().visit(this);

            if (condition)
                union.getRightArg().visit(this);
        }

        @Override
        public void meet(Join join) {
            condition = false;
        }

        @Override
        public void meet(Plan e) {
            if (e.getProperties().getSite().isRemote())
                condition = true;
            else
                e.getArg().visit(this);
        }

        @Override
        public void meet(SourceQuery query) {
            condition = true;
        }
    }

}
