package org.semagrow.util;

import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.plan.Plan;
import org.semagrow.plan.operators.BindJoin;
import org.semagrow.plan.operators.HashJoin;
import org.semagrow.plan.operators.MergeJoin;
import org.semagrow.plan.operators.SourceQuery;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class OrderPlanFixer extends AbstractQueryModelVisitor<RuntimeException> {

    public static void fixOrders(TupleExpr expr) {
        expr.visit(new OrderPlanFixer());
    }

    public void meet(Order order) {
        super.meet(order);

        Collection<String> vars = VarNameCollector.process(order.getArg());
        Collection<OrderElem> toRemove = new HashSet<>();

        for (OrderElem elem: order.getElements()) {
            if (elem.getExpr() instanceof Var) {
                if (!vars.contains(((Var) elem.getExpr()).getName())) {
                    toRemove.add(elem);
                }
            }
        }
        order.getElements().removeAll(toRemove);

        if (order.getElements().isEmpty()) {
            order.replaceWith(order.getArg());
        }
    }

    private static class VarNameCollector extends AbstractQueryModelVisitor<RuntimeException> {
        private Set<String> varNames = new HashSet();

        public VarNameCollector() {
        }

        public static Set<String> process(QueryModelNode node) {
            VarNameCollector collector = new VarNameCollector();
            node.visit(collector);
            return collector.getVarNames();
        }

        public Set<String> getVarNames() {
            return this.varNames;
        }

        @Override
        public void meet(Var var) {
            if (!var.hasValue()) {
                this.varNames.add(var.getName());
            }
        }

        @Override
        public void meet(ExtensionElem node) throws RuntimeException {
            this.varNames.add(node.getName());
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

        public void meet(SourceQuery node) {
            node.getArg().visit(this);
        }


    }
}

