package org.semagrow.plan.optimizer;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.semagrow.plan.Plan;
import org.semagrow.plan.operators.BindJoin;
import org.semagrow.plan.operators.HashJoin;
import org.semagrow.plan.operators.MergeJoin;
import org.semagrow.plan.operators.SourceQuery;

import java.util.HashSet;
import java.util.Set;

public class BindJoinExtensionOptimizer implements QueryOptimizer {
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
        tupleExpr.visit(new ExtensionFinder(tupleExpr));
    }

    protected static class ExtensionFinder extends AbstractQueryModelVisitor<RuntimeException> {
        protected final TupleExpr tupleExpr;

        public ExtensionFinder(TupleExpr tupleExpr) {
            this.tupleExpr = tupleExpr;
        }

        public void meet(Extension node) {
            super.meet(node);
            ExtensionRelocator.relocate(node);
        }
    }

    protected static class ExtensionRelocator extends AbstractQueryModelVisitor<RuntimeException> {
        protected final Extension extension;
        protected final Set<String> vars;

        public static void relocate(Extension extension) {
            extension.visit(new ExtensionRelocator(extension));
        }

        public ExtensionRelocator(Extension extension) {
            this.extension = extension;
            vars = new HashSet<>();

            for (ExtensionElem e : extension.getElements()) {
                vars.addAll(VarNameCollector.process(e.getExpr()));
            }
        }

        protected void meetNode(QueryModelNode node) {
            assert node instanceof TupleExpr;

            this.relocate(this.extension, (TupleExpr) node);
        }

        public void meet(SourceQuery node) {
            this.relocate(this.extension, node.getArg());
        }

        public void meet(Join join) {
            if (join instanceof BindJoin) {
                if (join.getLeftArg().getBindingNames().containsAll(this.vars)) {
                    join.getLeftArg().visit(this);
                } else {
                    join.getRightArg().visit(this);
                }
            }
            else {
                if (join.getLeftArg().getBindingNames().containsAll(this.vars)) {
                    join.getLeftArg().visit(this);
                } else if (join.getRightArg().getBindingNames().containsAll(this.vars)) {
                    join.getRightArg().visit(this);
                } else {
                    this.relocate(this.extension, join);
                }
            }
        }

        public void meet(LeftJoin leftJoin) {
            if (leftJoin.getLeftArg().getBindingNames().containsAll(this.vars)) {
                leftJoin.getLeftArg().visit(this);
            } else {
                this.relocate(this.extension, leftJoin);
            }
        }

        public void meet(Union union) {
            union.getLeftArg().visit(this);
            this.extension.setArg(union.getRightArg());
            union.getRightArg().replaceWith(this.extension);
            union.getRightArg().visit(this);
        }

        public void meet(Difference node) {
            Extension clone = new Extension();
            clone.setElements(extension.getElements());
            this.relocate(this.extension, node.getLeftArg());
            this.relocate(clone, node.getRightArg());
            relocate(this.extension);
            relocate(clone);
        }

        public void meet(Intersection node) {
            Extension clone = new Extension();
            clone.setElements(extension.getElements());
            this.relocate(this.extension, node.getLeftArg());
            this.relocate(clone, node.getRightArg());
            relocate(this.extension);
            relocate(clone);
        }

        public void meet(Extension node) {
            if (node.getArg().getBindingNames().containsAll(this.vars)) {
                node.getArg().visit(this);
            } else {
                this.relocate(this.extension, node);
            }
        }

        public void meet(EmptySet node) {
            if (this.extension.getParentNode() != null) {
                this.extension.replaceWith(this.extension.getArg());
            }
        }

        @Override
        public void meet(Projection node) throws RuntimeException {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Slice node) throws RuntimeException {
            node.getArg().visit(this);
        }

        public void meet(Filter filter) {
            filter.getArg().visit(this);
        }

        public void meet(Distinct node) {
            node.getArg().visit(this);
        }

        public void meet(Order node) {
            node.getArg().visit(this);
        }

        public void meet(QueryRoot node) {
            node.getArg().visit(this);
        }

        public void meet(Reduced node) {
            node.getArg().visit(this);
        }

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

        protected void relocate(Extension extension, TupleExpr newArg) {
            if (extension.getArg() != newArg) {
                if (extension.getParentNode() != null) {
                    extension.replaceWith(extension.getArg());
                }
                newArg.replaceWith(extension);
                extension.setArg(newArg);
            }
        }
    }
}
