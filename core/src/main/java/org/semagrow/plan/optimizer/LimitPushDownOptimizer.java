package org.semagrow.plan.optimizer;

import org.semagrow.plan.operators.SourceQuery;
import org.semagrow.plan.Plan;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * A semantically-preserved optimizer that pushes down @{link Limit} nodes
 * as far as possible. This optimizer is usefull in general since Limit
 * nodes can pushed down to the remote sources and therefore subqueries can
 * retrieve only limited query answers.
 * @author Angelos Charalambidis
 */
public class LimitPushDownOptimizer implements QueryOptimizer {

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new LimitFinder());
    }


    protected static class LimitFinder extends AbstractQueryModelVisitor<RuntimeException> {

        @Override
        public void meet(Slice slice) {
            super.meet(slice);
            LimitRelocator.relocate(slice);
        }
    }


    protected static class LimitRelocator extends AbstractQueryModelVisitor<RuntimeException> {

        protected Slice slice;

        protected LimitRelocator(Slice slice) { this.slice = slice; }

        public static void relocate(Slice slice) {
            slice.getArg().visit(new LimitRelocator(slice));
        }

        @Override
        protected void meetNode(QueryModelNode node) {

            assert node instanceof TupleExpr;

            if (node instanceof SourceQuery) {
                meet((SourceQuery) node);
            } else if (node instanceof Plan) {
                meet((Plan)node);
            } else {
                relocate(slice, (TupleExpr) node);
            }
        }

        @Override
        public void meet(Union union) {
            Slice sliceLeft = new Slice();
            Slice sliceRight = new Slice();
            sliceLeft.setLimit(slice.getLimit());
            sliceLeft.setOffset(slice.getOffset());
            sliceRight.setLimit(slice.getLimit());
            sliceRight.setOffset(slice.getOffset());

            relocate(sliceLeft, union.getLeftArg());
            relocate(sliceRight, union.getRightArg());
            LimitRelocator.relocate(sliceLeft);
            LimitRelocator.relocate(sliceRight);
        }

        @Override
        public void meet(Projection projection) { projection.getArg().visit(this); }

        @Override
        public void meet(Order order) { order.getArg().visit(this); }

        public void meet(Plan p) { p.getArg().visit(this); }

        public void meet(SourceQuery query) {
            Slice pushedSlice = slice;
            relocate(pushedSlice, query.getArg());
        }

        @Override
        public void meetOther(QueryModelNode node) {
            meetNode(node);
        }

        protected void relocate(Slice slice, TupleExpr newArg) {
            if (slice.getArg() != newArg) {
                if (slice.getParentNode() != null) {
                    // Remove filter from its original location
                    slice.replaceWith(slice.getArg());
                }

                // Insert filter at the new location
                newArg.replaceWith(slice);
                slice.setArg(newArg);
            }
        }
    }
}
