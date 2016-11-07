package org.semagrow.plan.querygraph;

import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.local.LocalSite;
import org.semagrow.plan.*;
import org.semagrow.selector.SourceSelector;

import java.util.*;

/**
 * Created by angel on 23/6/2016.
 */
public class QueryGraphPlanGenerator extends SimplePlanGenerator {

    private QueryGraphDecomposerContext ctx;


    private org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger( this.getClass() );


    public QueryGraphPlanGenerator(QueryGraphDecomposerContext ctx,
                                   SourceSelector selector,
                                   PlanFactory planFactory)
    {
        super(ctx, selector, planFactory);
        this.ctx = ctx;
    }

    @Override
    public PlanCollection joinPlans(PlanCollection p1, PlanCollection p2)
    {
        Collection<QueryPredicate> preds = getValidPredicatesFor(p1, p2);

        Set<TupleExpr> e = new HashSet<>(p1.getLogicalExpr());
        e.addAll(p2.getLogicalExpr());
        PlanCollection plans = new PlanCollectionImpl(e);

        for (QueryPredicate p : preds) {
            plans.addAll(combineWith(p1, p2, p));
        }

        return plans;

    }


    private Collection<QueryPredicate> getValidPredicatesFor(PlanCollection p1, PlanCollection p2) {

        if (p1.isEmpty() || p2.isEmpty())
            return Collections.emptyList();

        Set<TupleExpr> r1 = p1.getLogicalExpr();
        Set<TupleExpr> r2 = p2.getLogicalExpr();

        Collection<QueryPredicate> predicates = new LinkedList<>();

        Collection<QueryEdge> edges = ctx.getQueryGraph().getOutgoingEdges(r1);

        for (QueryEdge e : edges) {
            if (r2.contains(e.getTo())) {
                QueryPredicate p = e.getPredicate();
                Set<TupleExpr> r = new HashSet<>(r1);
                r.addAll(r2);
                if (p.canBeApplied(r))
                    predicates.add(p);
            }
        }
        return predicates;
    }


    public Collection<Plan> combineWith(PlanCollection p1, PlanCollection p2, QueryPredicate pred)
    {
        if (pred instanceof JoinPredicate)
            return combineWith(p1,p2, (JoinPredicate)pred);
        else if (pred instanceof LeftJoinPredicate)
            return combineWith(p1,p2, (LeftJoinPredicate)pred);
        else
            return Collections.emptyList();
    }


    public Collection<Plan> combineWith(PlanCollection p1, PlanCollection p2, JoinPredicate pred)
    {
        Collection<Plan> plans =  super.joinPlans(p1,p2);
        plans.addAll(super.joinPlans(p2,p1));
        return plans;
    }

    public Collection<Plan> combineWith(PlanCollection p1, PlanCollection p2, LeftJoinPredicate pred)
    {
        Collection<Plan> plans =  new LinkedList<Plan>();

        for (Plan pp1 : p1) {
            for (Plan pp2 : p2) {

                if (pp1.getProperties().getSite().isRemote() &&
                        pp2.getProperties().getSite().isRemote() &&
                        pp1.getProperties().getSite().equals(pp2.getProperties().getSite())) {

                    Plan ppp = create(new LeftJoin(pp1, pp2));
                    logger.debug("Plan added {}", ppp);
                    plans.add(ppp);
                }
                plans.add(create(new LeftJoin(enforce(pp1, LocalSite.getInstance()), enforce(pp2, LocalSite.getInstance()))));
            }
        }
        return plans;
    }

}
