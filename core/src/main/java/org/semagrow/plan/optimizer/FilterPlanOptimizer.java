package org.semagrow.plan.optimizer;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.semagrow.plan.AbstractPlanVisitor;
import org.semagrow.plan.Plan;
import org.semagrow.plan.operators.SourceQuery;

public class FilterPlanOptimizer implements QueryOptimizer {

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new FilterFinder(tupleExpr));
    }

    protected static class FilterFinder extends AbstractPlanVisitor<RuntimeException> {
        protected final TupleExpr tupleExpr;

        public FilterFinder(TupleExpr tupleExpr) {
            this.tupleExpr = tupleExpr;
        }

        public void meet(Filter filter) {
            super.meet(filter);
            FilterRelocator.relocate(filter);
        }
    }

    protected static class FilterRelocator extends AbstractPlanVisitor<RuntimeException> {
        protected final Filter filter;

        public static void relocate(Filter filter) {
            filter.visit(new FilterPlanOptimizer.FilterRelocator(filter));
        }

        public FilterRelocator(Filter filter) {
            this.filter = filter;
        }

        protected void meetNode(QueryModelNode node) {
            assert node instanceof TupleExpr;

            this.relocate(this.filter, (TupleExpr)node);
        }

        public void meet(Plan plan) {
            plan.getArg().visit(this);
        }

        public void meet(Union union) {
            Filter clone = new Filter();
            clone.setCondition(this.filter.getCondition().clone());
            this.relocate(this.filter, union.getLeftArg());
            this.relocate(clone, union.getRightArg());
            relocate(this.filter);
            relocate(clone);
        }

        public void meet(SourceQuery sourceQuery) {
            this.relocate(this.filter, sourceQuery.getArg());
        }

        public void meet(Filter filter) {
            filter.getArg().visit(this);
        }

        protected void relocate(Filter filter, TupleExpr newFilterArg) {
            if (filter.getArg() != newFilterArg) {
                if (filter.getParentNode() != null) {
                    filter.replaceWith(filter.getArg());
                }

                newFilterArg.replaceWith(filter);
                filter.setArg(newFilterArg);
            }

        }
    }
}
