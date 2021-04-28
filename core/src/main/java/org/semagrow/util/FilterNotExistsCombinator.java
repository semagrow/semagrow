package org.semagrow.util;

import org.semagrow.local.LocalSite;
import org.semagrow.plan.*;
import org.semagrow.plan.operators.BindNotExists;
import org.semagrow.plan.queryblock.Quantifier;

import java.util.Collection;
import java.util.HashSet;

public class FilterNotExistsCombinator {

    public static boolean match(Pair<Collection<Quantifier>, Collection<Plan>> left,
                                Pair<Collection<Quantifier>, Collection<Plan>> right) {

        if (left.getFirst().size() == 1 && right.getFirst().size() == 1) {
            boolean lqflag = getQuantifier(left).getQuantification().equals(Quantifier.Quantification.EACH);
            boolean rqflag = getQuantifier(right).getQuantification().equals(Quantifier.Quantification.ALL);

            return lqflag && rqflag;
        }
        return false;
    }

    public static Collection<Plan> combine(Pair<Collection<Quantifier>, Collection<Plan>> left,
                                           Pair<Collection<Quantifier>, Collection<Plan>> right,
                                           CompilerContext context) {
        Collection<Plan> collection = new HashSet<>();

        RequestedPlanProperties props = new RequestedPlanProperties();
        props.setSite(LocalSite.getInstance());
        Collection<Plan> ppl = context.enforceProps(left.getSecond(), props);
        Collection<Plan> ppr = context.enforceProps(right.getSecond(), props);

        for (Plan l: ppl) {
            for (Plan r: ppr) {
                collection.add(context.asPlan(new BindNotExists(l,r)));
            }
        }
        return collection;
    }

    private static Quantifier getQuantifier(Pair<Collection<Quantifier>, Collection<Plan>> pair) {
        if (pair.getFirst().size() == 1) {
            for (Quantifier q : pair.getFirst()) {
                return q;
            }
        }
        return null;
    }
}
