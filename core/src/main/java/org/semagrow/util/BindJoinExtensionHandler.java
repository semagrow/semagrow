package org.semagrow.util;

import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.plan.Plan;

import java.util.HashSet;
import java.util.Set;

public class BindJoinExtensionHandler {

    private static Set<Extension> extensions = new HashSet<>();

    public void extractExtensions(TupleExpr tupleExpr) {
        tupleExpr.visit(new ExtensionExtractor());
    }

    public void placeExtenstions(TupleExpr tupleExpr) {
        for (Extension extension: extensions) {
            ExtensionPlacer.place(tupleExpr, extension);
        }
    }

    protected static class ExtensionExtractor extends AbstractQueryModelVisitor<RuntimeException> {

        public void meet(Extension extension) {
            super.meet(extension);
            Extension e = extension.clone();
            e.setArg(new EmptySet());
            extensions.add(e);
        }
    }

    protected static class ExtensionPlacer extends AbstractQueryModelVisitor<RuntimeException> {
        private TupleExpr queryRoot;
        private Extension extension;
        private boolean placed = false;

        public static void place(TupleExpr tupleExpr, Extension extension) {
            QueryRoot queryRoot = new QueryRoot();
            queryRoot.setArg(tupleExpr);
            queryRoot.visit(new ExtensionPlacer(extension));
        }

        public ExtensionPlacer(Extension extension) {
            this.extension = extension;
        }

        public void meet(Extension extension) {
            super.meet(extension);
            if (this.extension.getElements().equals(extension.getElements())) {
                placed = true;
            }
        }

        @Override
        public void meet(QueryRoot queryRoot) throws RuntimeException {
            super.meet(queryRoot);
            if (!placed) {
                assert queryRoot.getArg() instanceof Plan;

                Plan rootPlan = (Plan) queryRoot.getArg();
                extension.setParentNode(rootPlan);
                extension.setArg(rootPlan.getArg());
                rootPlan.setArg(extension);
            }
        }
    }
}
