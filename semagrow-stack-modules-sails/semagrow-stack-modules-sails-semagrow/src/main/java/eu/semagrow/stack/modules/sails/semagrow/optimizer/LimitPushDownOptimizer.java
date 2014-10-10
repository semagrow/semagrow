package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * Created by angel on 6/26/14.
 */
public class LimitPushDownOptimizer implements QueryOptimizer {

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new LimitFinder());
    }


    protected static class LimitFinder extends QueryModelVisitorBase<RuntimeException> {

        @Override
        public void meet(Slice slice) {
            super.meet(slice);
            LimitRelocator.relocate(slice);
        }
    }


    protected static class LimitRelocator extends QueryModelVisitorBase<RuntimeException> {

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

            if (query.getSources().size() > 1) {
                // this will be a hidden union at runtime
                // so we must also retain the slice outside the query
                pushedSlice = new Slice();
                pushedSlice.setLimit(slice.getLimit());
                pushedSlice.setOffset(slice.getOffset());
            }
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
