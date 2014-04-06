package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SingleSourceExpr;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by angel on 3/14/14.
 */
public class SingleSourceClusterOptimizer implements QueryOptimizer {

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        List<TupleExpr> bgps = BasicGraphPatternExtractor.process(tupleExpr);
        for (TupleExpr bgp : bgps)
            optimizebgp(bgp);
    }

    private void optimizebgp(TupleExpr tupleExpr) {
        List<TupleExpr> exprs = JoinCollector.getJoinArgs(tupleExpr);
        Map<URI, List<SingleSourceExpr>> clusters = getClusters(exprs);
        TupleExpr expr = null;
        for (URI source : clusters.keySet()) {
            TupleExpr e = createSingleSourceJoin(clusters.get(source));
            if (expr == null)
                expr = e;
            else {
                expr = new Join(e, expr);
            }
        }
        tupleExpr.replaceWith(expr);
    }

    private Map<URI, List<SingleSourceExpr>> getClusters(List<TupleExpr> exprs) {
        Map<URI, List<SingleSourceExpr>> exprBySources = new HashMap<URI, List<SingleSourceExpr>>();

        for (TupleExpr e : exprs)
        {
            if (e instanceof SingleSourceExpr) {
                URI src = ((SingleSourceExpr)e).getSource();
                List<SingleSourceExpr> list = exprBySources.containsKey(src) ?
                        exprBySources.get(src) : new ArrayList<SingleSourceExpr>();
                list.add((SingleSourceExpr)e);
                exprBySources.put(src, list);
            }else {
                throw new IllegalArgumentException("Expression " + e.getSignature() + " is not single source");
            }
        }

        return exprBySources;
    }

    private SingleSourceExpr createSingleSourceJoin(List<SingleSourceExpr> joinArgs) {
        SingleSourceExpr result = null;

        for (SingleSourceExpr expr : joinArgs) {
            if (result == null)
                result = expr;
            else {
                URI s1 = expr.getSource();
                URI s2 = result.getSource();
                if (s1.equals(s2)) {
                    result = new SingleSourceExpr(new Join(expr.getArg(), result.getArg()), s1);
                } else {
                    throw new IllegalArgumentException("joinArgs have different sources " + s1.toString() + " and " + s2.toString());
                }
            }
        }
        return result;
    }

    protected static class JoinCollector  {

        public static List<TupleExpr> getJoinArgs(TupleExpr expr) {
            ArrayList<TupleExpr> exprs = new ArrayList<TupleExpr>();
            JoinCollector collector = new JoinCollector();
            return collector.getJoinArgs(expr, exprs);
        }

        protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
            if (tupleExpr instanceof Join) {
                Join join = (Join)tupleExpr;
                getJoinArgs(join.getLeftArg(), joinArgs);
                getJoinArgs(join.getRightArg(), joinArgs);
            }
            else {
                joinArgs.add(tupleExpr);
            }

            return joinArgs;
        }
    }

    public static class BasicGraphPatternExtractor extends QueryModelVisitorBase<RuntimeException> {

        private TupleExpr lastBGPNode;

        private List<TupleExpr> bgpList = new ArrayList<TupleExpr>();

        /**
         * Prevents creation of extractor classes.
         * The static process() method must be used instead.
         */
        private BasicGraphPatternExtractor() {}

        public static List<TupleExpr> process(QueryModelNode node) {
            BasicGraphPatternExtractor ex = new BasicGraphPatternExtractor();
            node.visit(ex);
            return ex.bgpList;
        }

        // --------------------------------------------------------------

        /**
         * Handles binary nodes with potential BGPs as children (e.g. union, left join).
         */
        @Override
        public void meetBinaryTupleOperator(BinaryTupleOperator node) throws RuntimeException {

            for (TupleExpr expr : new TupleExpr[] { node.getLeftArg(), node.getRightArg() }) {
                expr.visit(this);
                if (lastBGPNode != null) {
                    // child is a BGP node but this node is not
                    this.bgpList.add(lastBGPNode);
                    lastBGPNode = null;
                }
            }
        }

        /**
         * Handles unary nodes with a potential BGP as child (e.g. projection).
         */
        @Override
        public void meetUnaryTupleOperator(UnaryTupleOperator node) throws RuntimeException {

            node.getArg().visit(this);

            if (lastBGPNode != null) {
                if (node instanceof SingleSourceExpr) {
                    lastBGPNode = node;
                }
                else
                {
                    // child is a BGP node but this node is not
                    this.bgpList.add(lastBGPNode);
                    lastBGPNode = null;
                }
            }
        }

        /**
         * Handles statement patterns which are always a valid BGP node.
         */
        @Override
        public void meet(StatementPattern node) throws RuntimeException {
            this.lastBGPNode = node;
        }

        /*
        @Override
        public void meet(Filter filter) throws RuntimeException {
            // visit the filter argument but ignore the filter condition
            filter.getArg().visit(this);

            if (lastBGPNode != null) {
                // child is a BGP as well as the filter
                lastBGPNode = filter;
            }
        }*/

        @Override
        public void meet(Join join) throws RuntimeException {

            boolean valid = true;

            // visit join arguments and check that all are valid BGPS
            for (TupleExpr expr : new TupleExpr[] { join.getLeftArg(), join.getRightArg() }) {
                expr.visit(this);
                if (lastBGPNode == null) {
                    // child is not a BGP -> join is not a BGP
                    valid = false;
                } else {
                    if (!valid) {
                        // last child is a BGP but another child was not
                        this.bgpList.add(lastBGPNode);
                        lastBGPNode = null;
                    }
                }
            }
            if (valid)
                lastBGPNode = join;
        }
    }


    protected class SourceVisitor extends QueryModelVisitorBase<RuntimeException> {


        @Override
        public void meet(Join node) throws RuntimeException {
            //super.meet(node);
            meetNode(node);

        }

        @Override
        public void meetOther(QueryModelNode node) throws RuntimeException {
            super.meetOther(node);
        }

        @Override
        protected void meetNode(QueryModelNode node) throws RuntimeException {
            super.meetNode(node);
        }
    }


}
