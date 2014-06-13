package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SingleSourceExpr;
import eu.semagrow.stack.modules.sails.semagrow.algebra.SourceQuery;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.HashSet;
import java.util.Set;


/**
 * Push down projection into SingleSourceExpr expressions.
 * Only interesting variables will be projected.
 * This will eventually optimize the SPARQL subquery issued to the actual data source
 * and thus the bandwidth used.
 * @author acharal@iit.demokritos.gr
 */
@Deprecated
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
                addProjectionVars(((SingleSourceExpr) node).getArg());
            }
            else if (node instanceof SourceQuery){
                addProjectionVars(((SourceQuery) node).getArg());
            } else
                super.meetNode(node);
        }

        private void addProjectionVars(TupleExpr expr) {

            Set<String> vars = ProvidedVarsVisitor.process(expr);
            if (requestedVars != null) {
                vars.retainAll(requestedVars);
            }
            ProjectionElemList projectionList = new ProjectionElemList();
            for (String v : vars) {
                projectionList.addElement(new ProjectionElem(v));
            }
            TupleExpr expr1 = expr.clone();
            Projection projection = new Projection(expr1, projectionList);
            //expr.replaceWith(projection);
            expr.replaceWith(projection);
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
