package org.semagrow.plan.querygraph;

import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.semagrow.algebra.TupleExprs;

import java.util.*;

/**
 * Created by angel on 12/5/2015.
 */
public class QueryGraph
{
    private Collection<QueryEdge> edges = new LinkedList<QueryEdge>();

    private Collection<TupleExpr> vertices = new LinkedList<TupleExpr>();

    QueryGraph() { }

    public Collection<TupleExpr> getVertices() { return vertices; }

    public Collection<QueryEdge> getEdges() { return edges; }

    public Collection<QueryEdge> getOutgoingEdges(TupleExpr v) {
        Collection<QueryEdge> outgoingEdges = new LinkedList<QueryEdge>();
        for (QueryEdge e : getEdges()) {
            if (e.getFrom().equals(v))
                outgoingEdges.add(e);
        }
        return outgoingEdges;
    }

    public Collection<QueryEdge> getOutgoingEdges(Collection<TupleExpr> v) {

        Collection<QueryEdge> outgoingEdges = new LinkedList<>();

        for (TupleExpr e : v)
        {
            outgoingEdges.addAll(getOutgoingEdges(e));
        }
        return outgoingEdges;
    }

    public void addEdge(TupleExpr v1, TupleExpr v2, QueryPredicate pred) {

        QueryEdge e = new QueryEdge(v1,v2,pred);
        edges.add(e);
    }

    public void addVertex(TupleExpr expr) { this.vertices.add(expr); }

    public static QueryGraph create(TupleExpr expr) {
        return QueryGraphBuilder.build(expr);
    }

    private static class QueryGraphBuilder extends AbstractQueryModelVisitor<RuntimeException> {

        /* statements grouped by variable */
        private HashMap<String, Set<StatementPattern>> varMap = new HashMap<>();

        private QueryGraph graph;

        private Set<TupleExpr> tab = new HashSet<TupleExpr>();

        //private static Logger logger = LoggerFactory.getLogger(QueryGraphBuilder.class);

        private QueryGraphBuilder() {
            graph = new QueryGraph();
        }

        @Override
        public void meet(Join join)
        {
            Set<String> joinVars = commonVarNames(join.getLeftArg(), join.getRightArg());

            HashMap<String, Set<StatementPattern>> left = new HashMap<>();
            HashMap<String, Set<StatementPattern>> right = new HashMap<>();

            join.getLeftArg().visit(this);

        /* statement patterns that are on the left of the join and have vars in joinVars (i.e. with the right subtree) */
            for (String v : joinVars)
                left.put(v, new HashSet<StatementPattern>(varMap.get(v)));

            Set<TupleExpr> leftTab = new HashSet<TupleExpr>(tab);
            this.tab = new HashSet<>();

            join.getRightArg().visit(this);

            Set<TupleExpr> rightTab = new HashSet<TupleExpr>(tab);
            leftTab.addAll(rightTab);
            this.tab = leftTab;  // tab contain the union of rightTab and leftTab.

            for (String v : joinVars) {
                Set<StatementPattern> s = new HashSet<StatementPattern>(varMap.get(v));
                s.removeAll(left.get(v));
                right.put(v, s);
            }
            // right has all the statement patterns per common variable except the ones that are on the left.

            for (String v : joinVars) {
                for (StatementPattern p1 : left.get(v))
                {
                    for (StatementPattern p2 : right.get(v))
                    {
                        QueryPredicate p = new JoinPredicate(v);
                        Set<TupleExpr> eel = new HashSet<>();
                        eel.add(p1);
                        eel.add(p2);
                        p.setEEL(eel);
                        graph.addEdge(p1, p2, p);
                        //logger.debug("Adding INNER JOIN edge from " + p1 +  " to " + p2);
                    }
                }
            }
        }

        @Override
        public void meet(Filter filter) {
            filter.getArg().visit(this);
        }

        @Override
        public void meet(StatementPattern pattern) {

            Set<String> varNames = TupleExprs.getFreeVariables(pattern);

            for (String varName : varNames) {
                Set<StatementPattern> s = varMap.getOrDefault(varName, new HashSet<StatementPattern>());
                s.add(pattern);
                varMap.put(varName, s);
            }
            graph.addVertex(pattern);
            tab.add(pattern);
        }

        @Override
        public void meet(LeftJoin leftJoin) {
            Set<String> joinVars = commonVarNames(leftJoin.getLeftArg(), leftJoin.getRightArg());

            HashMap<String, Set<StatementPattern>> left = new HashMap<>();
            HashMap<String, Set<StatementPattern>> right = new HashMap<>();

            leftJoin.getLeftArg().visit(this);

            Set<TupleExpr> leftTab = new HashSet<TupleExpr>(tab);

            for (String v : joinVars)
                left.put(v, new HashSet<StatementPattern>(varMap.get(v)));

            this.tab = new HashSet<>();
            HashMap<String, Set<StatementPattern>> oldVarMap = this.varMap;
            this.varMap = new HashMap<>();


            leftJoin.getRightArg().visit(this);

            mergeMap(oldVarMap, this.varMap);
            this.varMap = oldVarMap;


            for (String v : joinVars) {
                Set<StatementPattern> s = new HashSet<StatementPattern>(varMap.get(v));
                s.removeAll(left.get(v));
                right.put(v, s);
            }

            Set<TupleExpr> rightTab = new HashSet<TupleExpr>(tab);
            leftTab.addAll(rightTab);
            this.tab = leftTab;

            for (String v : joinVars) {
                for (StatementPattern p1 : right.get(v)) {
                /*
                for (StatementPattern p2 : right.get(v)) {
                    QueryPredicate p = new LeftJoinPredicate(v);
                    Set<TupleExpr> eel = new HashSet<>();
                    eel.add(p1);
                    eel.add(p2);
                    eel.addAll(rightTab);
                    p.setEEL(eel);
                    graph.addEdge(p1, p2, p);
                }
                */

                    Set<StatementPattern> l = left.get(v);
                    if (!l.isEmpty())
                    {
                        StatementPattern ll = l.iterator().next();
                        QueryPredicate p = new LeftJoinPredicate(v);
                        Set<TupleExpr> eel = new HashSet<>();
                        eel.add(p1);
                        eel.add(ll);
                        eel.addAll(rightTab);
                        p.setEEL(eel);
                        graph.addEdge(ll, p1, p);
                    }
                }
            }
        }

        public static QueryGraph build(TupleExpr expr) {
            QueryGraphBuilder builder = new QueryGraphBuilder();
            expr.visit(builder);
            return builder.graph;
        }

        private Set<String> commonVarNames(TupleExpr leftArg, TupleExpr rightArg) {
            Set<String> leftVars  = TupleExprs.getFreeVariables(leftArg);
            Set<String> rightVars = TupleExprs.getFreeVariables(rightArg);
            leftVars.retainAll(rightVars);
            return leftVars;
        }

        private void mergeMap(HashMap<String, Set<StatementPattern>> oldVarMap,
                              HashMap<String, Set<StatementPattern>> varMap)
        {
            for (String v : varMap.keySet())
            {
                Set<StatementPattern> s = oldVarMap.get(v);

                if (s != null) {
                    s.addAll(varMap.getOrDefault(v, new HashSet<StatementPattern>()));
                } else {
                    s = varMap.get(v);
                }

                if (s != null) {
                    oldVarMap.put(v, s);
                }

            }
        }

    }
}