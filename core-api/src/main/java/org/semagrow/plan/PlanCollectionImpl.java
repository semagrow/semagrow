package org.semagrow.plan;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 *
 * @author acharal
 */
public class PlanCollectionImpl extends ArrayList<Plan> implements PlanCollection {


    private Set<TupleExpr> logical;

    public PlanCollectionImpl(TupleExpr e) {
        assert logical != null;
        this.logical = Collections.singleton(e);
    }

    public PlanCollectionImpl(Set<TupleExpr> logical) {
        this.logical = logical;
    }

    @Override
    public Set<TupleExpr> getLogicalExpr() {
        return logical;
    }


    public static Collector<Plan, ?, PlanCollection> toPlanCollection(TupleExpr logical) {
        return Collectors.toCollection(() -> new PlanCollectionImpl(logical));
    }

    public static Collector<Plan, ?, PlanCollection> toPlanCollection(Set<TupleExpr> logical) {
        return Collectors.toCollection(() -> new PlanCollectionImpl(logical));
    }

}
