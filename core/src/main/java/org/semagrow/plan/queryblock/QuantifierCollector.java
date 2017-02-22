package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by angel on 21/9/2016.
 */

public class QuantifierCollector extends AbstractQueryModelVisitor<RuntimeException> {

    private Collection<Quantifier.Var> vars = new LinkedList<>();

    @Override
    protected void meetNode(QueryModelNode node) throws RuntimeException {

        if (node instanceof Quantifier.Var)
            vars.add((Quantifier.Var)node);

        super.meetNode(node);
    }

    static Collection<Quantifier.Var> process(QueryModelNode node) {
        QuantifierCollector collector = new QuantifierCollector();
        node.visit(collector);
        return collector.vars;
    }
}