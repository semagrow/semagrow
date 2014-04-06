package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SingleSourceExpr;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.HashSet;
import java.util.Set;


/**
 * Created by angel on 3/15/14.
 */
public class SingleSourceProjectionOptimization implements QueryOptimizer {

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
            tupleExpr.visit(new ProjectionVisitor());
    }

    protected class ProjectionVisitor extends QueryModelVisitorBase<RuntimeException> {

        private HashSet<String> requestedVars = null;

        @Override
        public void meet(Projection projection) {

            if (requestedVars == null) {
                requestedVars = new HashSet<String>();
            }

            for (String target : projection.getProjectionElemList().getTargetNames())
                requestedVars.add(target);

            projection.getArg().visit(this);
        }

        @Override
        public void meet(Join join) {
            if (requestedVars != null) {
                Set<String> joinVars      = ProvidedVarsVisitor.process(join.getLeftArg());
                Set<String> rightJoinVars = ProvidedVarsVisitor.process(join.getRightArg());
                joinVars.retainAll(rightJoinVars); // get the variables where the join is performed

                requestedVars.addAll(joinVars);
                HashSet<String> requestedClone = (HashSet<String>)requestedVars.clone();
                join.getLeftArg().visit(this);

                requestedVars = requestedClone;
                join.getRightArg().visit(this);
            } else {
                join.visitChildren(this);
            }
        }

        @Override
        public void meetNode(QueryModelNode node) {
            if (node instanceof SingleSourceExpr) {
                TupleExpr expr = ((SingleSourceExpr) node).getArg();
                Set<String> vars = ProvidedVarsVisitor.process(expr);
                if (requestedVars != null) {
                    vars.retainAll(requestedVars);
                }
                ProjectionElemList projectionList = new ProjectionElemList();
                for (String v : vars) {
                    projectionList.addElement(new ProjectionElem(v));
                }
                Projection projection = new Projection(expr, projectionList);
                //expr.replaceWith(projection);
                node.replaceChildNode(expr, projection);
            }
            else
                super.meetNode(node);
        }
    }

    protected static class ProvidedVarsVisitor extends QueryModelVisitorBase<RuntimeException> {

        Set<String> varNames;

        public ProvidedVarsVisitor() {
            varNames = new HashSet<String>();
        }

        public static Set<String> process(TupleExpr expr) {
            ProvidedVarsVisitor visitor = new ProvidedVarsVisitor();
            expr.visit(visitor);
            return visitor.varNames;
        }

        @Override
        public void meet(Var var) {
            if (!var.hasValue()) {
                varNames.add(var.getName());
            }
        }

        @Override
        public void meet(Filter filter) {
            filter.getArg().visit(this);
        }

        @Override
        public void meet(Projection projection) {
            projection.getArg().visit(this);
            varNames.retainAll(projection.getProjectionElemList().getTargetNames());
        }
    }
}
