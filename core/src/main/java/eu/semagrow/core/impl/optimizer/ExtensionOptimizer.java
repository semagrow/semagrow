package eu.semagrow.core.impl.optimizer;

import eu.semagrow.commons.algebra.BindJoin;
import eu.semagrow.commons.algebra.HashJoin;
import eu.semagrow.commons.algebra.MergeJoin;
import eu.semagrow.commons.algebra.SourceQuery;
import eu.semagrow.core.impl.planner.Plan;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.VarNameCollector;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by angel on 25/8/2015.
 */
public class ExtensionOptimizer implements QueryOptimizer {


    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
        tupleExpr.visit(new ExtensionFinder(tupleExpr));
    }


    	/*--------------------------*
	 * Inner class ExtensionFinder *
	 *--------------------------*/

    protected static class ExtensionFinder extends QueryModelVisitorBase<RuntimeException> {

        protected final TupleExpr tupleExpr;

        public ExtensionFinder(TupleExpr tupleExpr) {
            this.tupleExpr = tupleExpr;
        }

        @Override
        public void meet(Extension filter) {
            super.meet(filter);
            ExtensionRelocator.relocate(filter);
        }
    }



	/*-----------------------------*
	 * Inner class ExtensionRelocator *
	 *-----------------------------*/

    protected static class ExtensionRelocator extends QueryModelVisitorBase<RuntimeException> {

        public static void relocate(Extension e) {
            e.visit(new ExtensionRelocator(e));
        }

        protected final Extension extension;

        protected final Set<String> vars;

        public ExtensionRelocator(Extension extension) {
            this.extension = extension;
            vars = new HashSet<String>();

            for (ExtensionElem e : extension.getElements())
                vars.addAll(VarNameCollector.process(e.getExpr()));

        }

        @Override
        protected void meetNode(QueryModelNode node) {
            // By default, do not traverse
            assert node instanceof TupleExpr;
            relocate(extension, (TupleExpr) node);
        }

        @Override
        public void meet(Join join) {
            if (join.getLeftArg().getBindingNames().containsAll(vars)) {
                // All required vars are bound by the left expr
                join.getLeftArg().visit(this);
            }
            else if (join.getRightArg().getBindingNames().containsAll(vars)) {
                // All required vars are bound by the right expr
                join.getRightArg().visit(this);
            }
            else {
                relocate(extension, join);
            }
        }

        @Override
        public void meet(LeftJoin leftJoin) {
            if (leftJoin.getLeftArg().getBindingNames().containsAll(vars)) {
                leftJoin.getLeftArg().visit(this);
            }
            else {
                relocate(extension, leftJoin);
            }
        }

        @Override
        public void meet(Union union) {
            Extension clone = new Extension();
            clone.setElements(extension.getElements());

            relocate(extension, union.getLeftArg());
            relocate(clone, union.getRightArg());

            ExtensionRelocator.relocate(extension);
            ExtensionRelocator.relocate(clone);
        }

        @Override
        public void meet(Difference node) {
            Extension clone = new Extension();
            clone.setElements(extension.getElements());

            relocate(extension, node.getLeftArg());
            relocate(clone, node.getRightArg());

            ExtensionRelocator.relocate(extension);
            ExtensionRelocator.relocate(clone);
        }

        @Override
        public void meet(Intersection node) {
            Extension clone = new Extension();
            clone.setElements(extension.getElements());

            relocate(extension, node.getLeftArg());
            relocate(clone, node.getRightArg());

            ExtensionRelocator.relocate(extension);
            ExtensionRelocator.relocate(clone);
        }

        @Override
        public void meet(Extension node) {
            if (node.getArg().getBindingNames().containsAll(vars)) {
                node.getArg().visit(this);
            }
            else {
                relocate(extension, node);
            }
        }

        @Override
        public void meet(EmptySet node) {
            if (extension.getParentNode() != null) {
                // Remove filter from its original location
                extension.replaceWith(extension.getArg());
            }
        }

        @Override
        public void meet(Filter filter) {
            // Filters are commutative
            filter.getArg().visit(this);
        }

        @Override
        public void meet(Distinct node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Order node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(QueryRoot node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Reduced node) {
            node.getArg().visit(this);
        }

        public void meet(SourceQuery node) { node.getArg().visit(this); }

        @Override
        public void meetOther(QueryModelNode node) {
            if (node instanceof Plan)
                ((Plan) node).getArg().visit(this);
            else if (node instanceof BindJoin)
                meet((Join)node);
            else if (node instanceof HashJoin)
                meet((Join)node);
            else if (node instanceof MergeJoin)
                meet((Join)node);
            else if (node instanceof SourceQuery)
                meet((SourceQuery)node);
            else
                meetNode(node);
        }

        protected void relocate(Extension e, TupleExpr newFilterArg) {
            if (e.getArg() != newFilterArg) {
                if (e.getParentNode() != null) {
                    // Remove filter from its original location
                    e.replaceWith(e.getArg());
                }

                // Insert filter at the new location
                newFilterArg.replaceWith(e);
                e.setArg(newFilterArg);
            }
        }
    }
}
