package org.semagrow.geospatial.disjoint;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.GEOF;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.VarNameCollector;
import org.eclipse.rdf4j.repository.Repository;
import org.semagrow.estimator.DisjointCheker;
import org.semagrow.plan.Plan;
import org.semagrow.plan.operators.SourceQuery;

import java.util.*;

public class GeoDisjointChecker implements DisjointCheker {

    GeoDisjointBase base = new GeoDisjointBase();

    private static final Set<String> nonDisjointStRelations;

    static {
        nonDisjointStRelations = new HashSet<>();

        nonDisjointStRelations.add(GEOF.SF_EQUALS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_INTERSECTS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_TOUCHES.stringValue());
        nonDisjointStRelations.add(GEOF.SF_WITHIN.stringValue());
        nonDisjointStRelations.add(GEOF.SF_CONTAINS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_OVERLAPS.stringValue());
        nonDisjointStRelations.add(GEOF.SF_CROSSES.stringValue());
    }

    public GeoDisjointChecker(Repository metadata) {
        base.setMetadata(metadata);
    }

    @Override
    public boolean areDisjoint(TupleExpr expr1, TupleExpr expr2) {
        if (expr1 instanceof SourceQuery && expr2 instanceof SourceQuery) {
            return areDisjoint((SourceQuery) expr1, (SourceQuery)expr2);
        }
        return false;
    }

    private boolean areDisjoint(SourceQuery q1, SourceQuery q2) {

        Collection<String> vars1 = VarNameCollector.process(q1);
        Collection<String> vars2 = VarNameCollector.process(q2);

        Resource endpoint1 = q1.getSite().getID();
        Resource endpoint2 = q2.getSite().getID();

        Collection<String> commonvars = vars1;
        commonvars.retainAll(vars2);

        for (String var: commonvars) {
            if (GeoDisjointChecker.VarPositionFinder.isSubjectVar(var, q1)) {
                return base.areDisjointThematic(endpoint1, endpoint2);
            }
            if (GeoDisjointChecker.VarPositionFinder.isSubjectVar(var, q2)) {
                return base.areDisjointThematic(endpoint1, endpoint2);
            }
            if (GeoDisjointChecker.VarPositionFinder.isGeoFilterVar(var, q1)) {
                return base.areDisjointSpatial(endpoint1, endpoint2);
            }
            if (GeoDisjointChecker.VarPositionFinder.isGeoFilterVar(var, q2)) {
                return base.areDisjointSpatial(endpoint1, endpoint2);
            }
        }
        return false;
    }

    protected static class VarPositionFinder extends AbstractQueryModelVisitor<RuntimeException> {
        List<Boolean> foundSubject = new ArrayList<>();
        List<Boolean> foundGeoFilter = new ArrayList<>();
        String varName;

        private VarPositionFinder(String varName) {
            this.varName = varName;
        }

        public static boolean isSubjectVar(String varName, TupleExpr expr){
            VarPositionFinder finder = new VarPositionFinder(varName);
            expr.visit(finder);
            return !finder.foundSubject.isEmpty();
        }

        public static boolean isGeoFilterVar(String varName, TupleExpr expr){
            VarPositionFinder finder = new VarPositionFinder(varName);
            expr.visit(finder);
            return !finder.foundGeoFilter.isEmpty();
        }

        public void meet(StatementPattern node) throws RuntimeException {
            if (node.getSubjectVar().getName().equals(varName)) {
                foundSubject.add(true);
            }
        }

        public void meet(Filter node) {
            if (node.getCondition() instanceof FunctionCall) {
                FunctionCall f = (FunctionCall) node.getCondition();
                if (nonDisjointStRelations.contains(f.getURI())) {
                    if (f.getArgs().get(0) instanceof Var) {
                        Var var = (Var) f.getArgs().get(0);
                        if (var.getName().equals(varName)) {
                            foundGeoFilter.add(true);
                        }
                    }
                    if (f.getArgs().get(1) instanceof Var) {
                        Var var = (Var) f.getArgs().get(1);
                        if (var.getName().equals(varName)) {
                            foundGeoFilter.add(true);
                        }
                    }
                }
                node.getArg().visit(this);
            }
        }

        @Override
        public void meetOther(QueryModelNode node) {
            if (node instanceof Plan)
                ((Plan) node).getArg().visit(this);
            else
                meetNode(node);
        }
    }
}
