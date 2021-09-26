package org.semagrow.plan.optimizer;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.estimator.DisjointCheker;
import org.semagrow.plan.Plan;
import org.semagrow.plan.operators.BindJoin;
import org.semagrow.plan.operators.SourceQuery;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class DisjointUnionJoinOptimizer implements QueryOptimizer {
    private DisjointCheker disjointChecker;

    public DisjointUnionJoinOptimizer(DisjointCheker disjointCheker) {
        this.disjointChecker = disjointCheker;
    }

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
        System.out.println(tupleExpr);
        boolean b = true;
        while (b) {
            b = BindJoinFinder.process(disjointChecker, tupleExpr);
        }
    }

    protected static class SourceQueryCollector extends AbstractQueryModelVisitor<RuntimeException> {
        protected final List<SourceQuery> sourceQueries;

        private SourceQueryCollector() {
            sourceQueries = new ArrayList<>();
        }

        public static List<SourceQuery> process(TupleExpr expr){
            SourceQueryCollector collector = new SourceQueryCollector();
            expr.visit(collector);
            return collector.sourceQueries;
        }

        public void meet(SourceQuery node) {
            sourceQueries.add(node);
        }

        public void meet(Union node) {
            node.getLeftArg().visit(this);
            node.getRightArg().visit(this);
        }

        public void meet(Plan node) {
            node.getArg().visit(this);
        }

        public void meet(Join node) {

        }

        @Override
        public void meetOther(QueryModelNode node) {
            if (node instanceof Plan)
                ((Plan) node).getArg().visit(this);
            else if (node instanceof SourceQuery)
                meet((SourceQuery)node);
            else
                meetNode(node);
        }
    }

    protected static class BindJoinFinder extends AbstractQueryModelVisitor<RuntimeException> {
        protected DisjointCheker disjointChecker;
        protected final List<SourceQuery> sourceQueries;

        private BindJoinFinder(DisjointCheker disjointCheker) {
            this.sourceQueries = new ArrayList<>();
            this.disjointChecker = disjointCheker;
        }

        public static boolean process(DisjointCheker disjointCheker, TupleExpr expr){
            BindJoinFinder finder = new BindJoinFinder(disjointCheker);
            expr.visit(finder);
            return !finder.sourceQueries.isEmpty();
        }

        public void meet(Join node) {
            assert node instanceof BindJoin;

            if (node.getLeftArg() instanceof Plan && ((Plan) node.getLeftArg()).getArg() instanceof BindJoin) {
                BindJoin left = (BindJoin) ((Plan) node.getLeftArg()).getArg();

                List<SourceQuery> lSourceQueries = SourceQueryCollector.process(left.getRightArg());
                List<SourceQuery> rSourceQueries = SourceQueryCollector.process(node.getRightArg());

                if (canApplyOptimization(disjointChecker, lSourceQueries, rSourceQueries)) {
                    groupSourceQueries(lSourceQueries, rSourceQueries);
                    node.replaceWith(new BindJoin(left.getLeftArg(), unionize(sourceQueries)));
                }
                else {
                    node.getLeftArg().visit(this);
                }
            }
            else {

                List<SourceQuery> lSourceQueries = SourceQueryCollector.process(node.getLeftArg());
                List<SourceQuery> rSourceQueries = SourceQueryCollector.process(node.getRightArg());

                if (canApplyOptimization(disjointChecker, lSourceQueries, rSourceQueries)) {
                    groupSourceQueries(lSourceQueries, rSourceQueries);
                    node.replaceWith(unionize(sourceQueries));
                }
            }
        }

        @Override
        public void meetOther(QueryModelNode node) {
            if (node instanceof Plan)
                ((Plan) node).getArg().visit(this);
            else
                meetNode(node);
        }

        private static boolean canApplyOptimization(DisjointCheker disjointCheker, List<SourceQuery> lSourceQueries, List<SourceQuery> rSourceQueries) {
            if (lSourceQueries.isEmpty()) {
                return false;
            }
            for (SourceQuery sq1: lSourceQueries) {
                for (SourceQuery sq2: rSourceQueries) {
                    if (!sq1.getSite().getID().equals(sq2.getSite().getID())) {
                        if (!disjointCheker.areDisjoint(sq1, sq2)) {
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        private void groupSourceQueries(List<SourceQuery> lSourceQueries, List<SourceQuery> rSourceQueries) {
            sourceQueries.clear();
            for (SourceQuery sq1 : lSourceQueries) {
                for (SourceQuery sq2 : rSourceQueries) {
                    if (sq1.getSite().getID().equals(sq2.getSite().getID())) {
                        Join j = new Join(sq1.getArg(), sq2.getArg());
                        SourceQuery sq = new SourceQuery(j, sq1.getSite());
                        sourceQueries.add(sq);
                    }
                }
            }
        }

        private static TupleExpr unionize(List<SourceQuery> exprs) {
            if (exprs.isEmpty()) {
                return new EmptySet();
            }
            else {
                if (exprs.size() == 1) {
                    return exprs.get(0);
                }
                else {
                    return new Union(unionize(exprs.subList(1, exprs.size())), exprs.get(0));
                }
            }
        }
    }
}
