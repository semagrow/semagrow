package org.semagrow.plan.util;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.semagrow.plan.AbstractPlanVisitor;
import org.semagrow.plan.operators.SourceQuery;

import java.util.Collection;
import java.util.HashSet;

public class EndpointCollector extends AbstractPlanVisitor<RuntimeException> {

    private Collection<String> endpoints;

    private EndpointCollector() {
        endpoints = new HashSet<>();
    }

    public static Collection<String> process(TupleExpr expr){
        EndpointCollector endpointCollector = new EndpointCollector();
        expr.visit(endpointCollector);
        return endpointCollector.endpoints;
    }

    public void meet(SourceQuery sourceQuery) {
        endpoints.add(sourceQuery.getSite().toString());
    }
}
