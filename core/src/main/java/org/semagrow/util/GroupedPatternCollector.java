package org.semagrow.util;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

public class GroupedPatternCollector extends AbstractQueryModelVisitor<RuntimeException> {

    private Collection<List<StatementPattern>> groups = new ArrayList<>();

    private GroupedPatternCollector() { }

    public static Collection<List<StatementPattern>> process(TupleExpr expr){
        GroupedPatternCollector collector = new GroupedPatternCollector();
        expr.visit(collector);
        if (collector.groups.isEmpty()) {
            collector.addGroup(StatementPatternCollector.process(expr));
        }
        return collector.groups;
    }

    @Override
    public void meet(Union node) throws RuntimeException {

        TupleExpr l = node.getLeftArg();
        TupleExpr r = node.getRightArg();

        if (l instanceof Union) {
            l.visit(this);
        }
        else  {
            groups.add(StatementPatternCollector.process(l));
        }

        if (r instanceof Union) {
            r.visit(this);
        }
        else  {
            groups.add(StatementPatternCollector.process(r));
        }
    }

    public void addGroup(List<StatementPattern> group) {
        groups.add(group);
    }
}
